/*
 * Copyright 2015 Steve Jones. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.github.sjones4.scale

import com.amazonaws.AmazonServiceException
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.services.ec2.model.*
import com.github.sjones4.youcan.youtwo.YouTwo
import com.github.sjones4.youcan.youtwo.YouTwoClient
import com.github.sjones4.youcan.youtwo.model.DescribeInstanceTypesRequest
import com.github.sjones4.youcan.youtwo.model.InstanceType
import com.github.sjones4.youcan.youtwo.model.InstanceTypeZoneStatus
import org.junit.Test

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail

/**
 * Scale test for churn of EC2 instances in a single account.
 */
class ScaleEC2InstancesChurnTest {

  private final AWSCredentialsProvider eucalyptusCredentials

  ScaleEC2InstancesChurnTest( ) {
    this.eucalyptusCredentials = new StaticCredentialsProvider( new BasicAWSCredentials(
        Objects.toString( System.getenv('AWS_ACCESS_KEY_ID'),     System.getenv('AWS_ACCESS_KEY') ),
        Objects.toString( System.getenv('AWS_SECRET_ACCESS_KEY'), System.getenv('AWS_SECRET_KEY') )
    ) )
  }

  private String cloudUri( String env, String servicePath ) {
    String url = System.getenv( env )
    assertNotNull( "Expected URL from environment (${env})", url )
    URI.create( url )
        .resolve( servicePath )
        .toString()
  }

  private YouTwo getEC2Client( final AWSCredentialsProvider credentials = eucalyptusCredentials ) {
    final YouTwo ec2 = new YouTwoClient( credentials, new ClientConfiguration(
        socketTimeout: TimeUnit.MINUTES.toMillis( 2 )
    ) )
    ec2.setEndpoint( cloudUri( 'EC2_URL', '/services/compute' ) )
    ec2
  }

  private void print( String text ) {
    System.out.println( text )
  }

  @Test
  void test( ) {
    final YouTwo ec2 = getEC2Client( )

    // Find an AZ to use
    final DescribeAvailabilityZonesResult azResult = ec2.describeAvailabilityZones();

    assertTrue( 'Availability zone not found', azResult.getAvailabilityZones().size() > 0 );

    String availabilityZone = azResult.getAvailabilityZones().get( 0 ).getZoneName();

    // Find an image to use
    final String imageId = ec2.describeImages( new DescribeImagesRequest(
        filters: [
            new Filter( name: "image-type", values: ["machine"] ),
            new Filter( name: "root-device-type", values: ["instance-store"] ),
            new Filter( name: "is-public", values: ["true"] ),
        ]
    ) ).with {
      images?.getAt( 0 )?.imageId
    }
    assertNotNull( 'Image not found', imageId )
    print( "Using image: ${imageId}" )

    // Find instance type with max capacity
    Integer availability = null
    final String instanceType = ec2.describeInstanceTypes( new DescribeInstanceTypesRequest( availability: true ) ).with {
      instanceTypes.inject( (InstanceType)null ) { InstanceType maxAvailability, InstanceType item ->
        def zoneClosure = {
          InstanceTypeZoneStatus max, InstanceTypeZoneStatus cur ->
            max != null && max.available > cur.available ? max : cur
        }
        InstanceTypeZoneStatus maxZoneStatus = maxAvailability?.availability?.inject( (InstanceTypeZoneStatus)null, zoneClosure )
        InstanceTypeZoneStatus itemZoneStatus = item.availability.inject( (InstanceTypeZoneStatus)null, zoneClosure )
        if ( maxZoneStatus != null && maxZoneStatus.available > itemZoneStatus.available ) {
          availability = maxZoneStatus.available
          maxAvailability
        } else {
          availability = itemZoneStatus.available
          item
        }
      }?.name
    }
    assertNotNull( 'Instance type not found', instanceType )
    assertNotNull( 'Availability zone not found', availabilityZone )
    assertNotNull( 'Availability not found', availability )
    print( "Using instance type ${instanceType} and availability zone ${availabilityZone} with availability ${availability}" );
    
    //
    ec2.describeAddresses( new DescribeAddressesRequest( publicIps: [ 'verbose' ] ) ).with {
      int addressCount = addresses.count { Address address ->
        'nobody' == address?.instanceId  
      }
      if ( addressCount < availability ) {
        print( "WARNING: Insufficient addresses ${addressCount} for available instances ${availability}" )
        availability = addressCount
      }
      void
    }
    
    // Find a key pair
    final String key = ec2.describeKeyPairs( ).with {
      keyPairs?.getAt( 0 )?.keyName
    }
    print( "Using key: ${key}" )
    
    final String namePrefix = UUID.randomUUID().toString().substring(0, 13) + "-";
    print( "Using resource prefix for test: " + namePrefix );

    final long startTime = System.currentTimeMillis( )
    final List<List<Runnable>> allCleanupTasks = new ArrayList<>( )
    try {
      final int threads = availability > 50 ? 50 : availability
      final int iterations = 20
      print( "Churning ${iterations} instances using ${threads} threads" )
      final CountDownLatch latch = new CountDownLatch( threads )
      ( 1..threads ).each { Integer thread ->
        final List<Runnable> cleanupTasks = [] as List<Runnable>
        allCleanupTasks << cleanupTasks
        Thread.start {
          getEC2Client( ).with {
            try {
              (1..iterations).each { Integer count ->
                print( "[${thread}] Running instance ${count}" )
                String instanceId
                runInstances( new RunInstancesRequest(
                    imageId: imageId,
                    instanceType: instanceType,
                    placement: new Placement(
                        availabilityZone: availabilityZone
                    ),
                    keyName: key,
                    minCount: 1,
                    maxCount: 1,
                    clientToken: "${namePrefix}${thread}-${count}"
                ) ).with {
                  reservation?.instances?.each{ Instance instance ->
                    cleanupTasks.add{
                      terminateInstances( new TerminateInstancesRequest(
                        instanceIds: [ instance.instanceId ]
                      ) )
                    }
                    instanceId = instance.instanceId
                  }
                }
                
                (1..100).find { Integer iter ->
                  sleep( 5000 )
                  print( "[${thread}] Waiting for instance ${count} ${instanceId} to be running (${5*iter}s)" )
                  String instanceState = describeInstances( new DescribeInstancesRequest( 
                    filters: [
                        new Filter( name: 'instance-state-name', values: [ 'pending', 'running' ] ),
                        new Filter( name: 'instance-id', values: [ instanceId ] ),
                    ]
                  ) ).with {
                    String resState = null
                    reservations?.each{ Reservation reservation ->
                      reservation?.instances?.each { Instance instance ->
                        if ( instance.instanceId == instanceId ) {
                          resState = instance?.state?.name
                        }
                      }
                    }
                    resState
                  }
                  if ( instanceState == 'running' ) {
                    instanceState
                  } else if ( instanceState == 'pending' ) {
                    null
                  } else if ( instanceState == null && iter < 5 ) {
                    print( "[${thread}] Null instance state ${count} ${instanceId}, treating as pending" )
                    null
                  } else if ( instanceState == null ) {
                    print( "[${thread}] Null instance state ${count} ${instanceId}, treating as running (will attempt terminate)" )
                    'running'
                  } else {
                    fail( "Unexpected instance ${count} ${instanceId} state ${instanceState}"  )
                  }
                }

                print( "[${thread}] Terminating instance ${count} ${instanceId}" )
                terminateInstances( new TerminateInstancesRequest(
                    instanceIds: [ instanceId ]
                ) )

                (1..100).find { Integer iter ->
                  sleep( 5000 )
                  print( "[${thread}] Waiting for instance ${count} ${instanceId} to be terminated (${5*iter}s)" )
                  String instanceState = describeInstances( new DescribeInstancesRequest(
                      filters: [
                          new Filter( name: 'instance-state-name', values: [ 'shutting-down', 'terminated' ] ),
                          new Filter( name: 'instance-id', values: [ instanceId ] ),
                      ]
                  ) ).with {
                    String resState = null
                    reservations?.each{ Reservation reservation ->
                      reservation?.instances?.each { Instance instance ->
                        if ( instance.instanceId == instanceId ) {
                          resState = instance?.state?.name
                        }
                      }
                    }
                    resState
                  }
                  if ( instanceState == 'terminated' ) {
                    instanceState
                  } else if ( instanceState == 'shutting-down' ) {
                    null
                  } else if ( instanceState == null ) {
                    print( "[${thread}] Null instance state ${count} ${instanceId}, treating as terminated" )
                    'terminated'
                  } else {
                    println( "Unexpected instance ${count} ${instanceId} state ${instanceState}"  )
                    instanceState // try to continue?
                  }
                }
              }
            } finally {
              latch.countDown( )
            }
          }
        }
      }
      latch.await( )

      print( "Test complete in ${System.currentTimeMillis()-startTime}ms" )
    } finally {
      // Attempt to clean up anything we created
      print( "Running cleanup tasks" )
      final long cleanupStart = System.currentTimeMillis( )
      final CountDownLatch cleanupLatch = new CountDownLatch( allCleanupTasks.size( ) )
      allCleanupTasks.each { List<Runnable> cleanupTasks ->
        Thread.start {
          try {
            cleanupTasks.reverseEach { Runnable cleanupTask ->
              try {
                cleanupTask.run()
              } catch ( AmazonServiceException e ) {
                print( "${e.serviceName}/${e.errorCode}: ${e.errorMessage}" )
              } catch ( Exception e ) {
                e.printStackTrace( )
              }
            }
          } finally {
            cleanupLatch.countDown( )
          }
        }
      }
      cleanupLatch.await( )
      print( "Completed cleanup tasks in ${System.currentTimeMillis()-cleanupStart}ms" )
    }
  }
}
