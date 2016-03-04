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
import com.amazonaws.Request
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.handlers.AbstractRequestHandler
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.services.autoscaling.AmazonAutoScaling
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient
import com.amazonaws.services.autoscaling.model.*
import com.amazonaws.services.ec2.AmazonEC2
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult
import com.amazonaws.services.ec2.model.DescribeImagesRequest
import com.amazonaws.services.ec2.model.Filter
import com.amazonaws.services.identitymanagement.model.CreateAccessKeyRequest
import com.github.sjones4.youcan.youare.YouAreClient
import com.github.sjones4.youcan.youare.model.CreateAccountRequest
import com.github.sjones4.youcan.youare.model.DeleteAccountRequest
import org.junit.Test

import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue

/**
 * Scale test for auto scaling groups with instances in multiple accounts.
 *
 * https://eucalyptus.atlassian.net/browse/EUCA-11125
 */
class ScaleASGroupsAccountsInstancesTest {

  private final AWSCredentialsProvider eucalyptusCredentials

  ScaleASGroupsAccountsInstancesTest( ) {
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

  private YouAreClient getYouAreClient( final AWSCredentialsProvider credentials = eucalyptusCredentials ) {
    final YouAreClient euare = new YouAreClient( credentials )
    euare.setEndpoint( cloudUri( 'AWS_IAM_URL', '/services/Euare' ) )
    euare
  }

  private AmazonEC2 getEC2Client( final AWSCredentialsProvider credentials = eucalyptusCredentials ) {
    final AmazonEC2 ec2 = new AmazonEC2Client( credentials )
    ec2.setEndpoint( cloudUri( 'EC2_URL', '/services/compute' ) )
    ec2
  }

  private AmazonAutoScaling getASClient( final AWSCredentialsProvider credentials = eucalyptusCredentials ) {
    final AmazonAutoScaling aas = new AmazonAutoScalingClient( credentials )
    aas.setEndpoint( cloudUri( 'AWS_AUTO_SCALING_URL', '/services/AutoScaling' ) )
    aas
  }

  private void print( String text ) {
    System.out.println( text )
  }

  @Test
  void test( ) {
    final AmazonEC2 ec2 = getEC2Client( )

    // Find an AZ to use
    final DescribeAvailabilityZonesResult azResult = ec2.describeAvailabilityZones();

    assertTrue( 'Availability zone not found', azResult.getAvailabilityZones().size() > 0 );

    final String availabilityZone = azResult.getAvailabilityZones().get( 0 ).getZoneName();
    print( "Using availability zone: " + availabilityZone );

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
    assertNotNull( 'Image not found', imageId != null )
    print( "Using image: ${imageId}" )

    final String namePrefix = UUID.randomUUID().toString().substring(0, 13) + "-";
    print( "Using resource prefix for test: " + namePrefix );

    final long startTime = System.currentTimeMillis( )
    final List<LinkedBlockingQueue<Runnable>> allCleanupTasks = new ArrayList<>( )
    try {
      final int threads = 50
      final int accounts = 500
      final int groups = 4
      final int instancesPerGroup = 1
      final int iterations = accounts / threads
      print( "Creating ${groups} auto scaling group(s) of ${instancesPerGroup} instance(s) in ${accounts} account(s) using ${threads} thread(s)" )
      final CountDownLatch latch = new CountDownLatch( threads )
      ( 1..threads ).each { Integer thread ->
        final LinkedBlockingQueue<Runnable> cleanupTasks = new LinkedBlockingQueue<Runnable>( )
        allCleanupTasks << cleanupTasks
        Thread.start {
          try{
            ( 1..iterations ).each { Integer account ->
              final AtomicBoolean launchConfigDeleted = new AtomicBoolean( false );
              final String accountName = "${namePrefix}account-${thread}-${account}"
              getYouAreClient( ).with {
                // Create account for testing
                print("[${thread}] Creating account ${accountName}")
                createAccount(new CreateAccountRequest(accountName: accountName))
                Closure accclo
                cleanupTasks.add accclo = {
                  if ( launchConfigDeleted.get( ) ) {
                    print("Deleting account: ${accountName}")
                    deleteAccount(new DeleteAccountRequest(accountName: accountName, recursive: true))
                  } else {
                    print("Rescheduling cleanup for account ${accountName}")
                    cleanupTasks.add( accclo )
                  }
                }
              }

              // Get credentials for new account
              AWSCredentialsProvider accountCredentials = getYouAreClient().with {
                addRequestHandler(new AbstractRequestHandler() {
                  public void beforeRequest(final Request<?> request) {
                    request.addParameter("DelegateAccount", accountName)
                  }
                })
                createAccessKey(new CreateAccessKeyRequest(userName: "admin")).with {
                  accessKey?.with {
                    new StaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretAccessKey))
                  }
                }
              }
              assertNotNull("[${thread}] Expected account credentials", accountCredentials)

              getASClient( accountCredentials ).with {
                final String launchConfigName = "${namePrefix}config-${thread}"
                print( "[${thread}] Creating launch configuration ${launchConfigName}" )
                createLaunchConfiguration(new CreateLaunchConfigurationRequest(
                    instanceType: 't1.micro',
                    imageId: imageId,
                    launchConfigurationName: launchConfigName
                ))
                Closure lcclo
                cleanupTasks.add lcclo = {
                  try {
                    deleteLaunchConfiguration(new DeleteLaunchConfigurationRequest(
                        launchConfigurationName: launchConfigName
                    ))
                    launchConfigDeleted.set( true )
                  } catch ( AmazonServiceException e ) {
                    if ( 'ResourceInUse' == e.errorCode ) {
                      print( "Rescheduling cleanup for launch configuration ${launchConfigName}" )
                      cleanupTasks.add( lcclo )
                    } else {
                      throw e
                    }
                  }
                }

                print( "[${thread}] Creating ${groups} auto scaling groups for account ${accountName}" )
                (1..groups).each { Integer count ->
                  final String groupName = "${namePrefix}group-${thread}-${count}"
                  createAutoScalingGroup(new CreateAutoScalingGroupRequest(
                      autoScalingGroupName: groupName,
                      minSize: 0,
                      maxSize: instancesPerGroup,
                      desiredCapacity: instancesPerGroup,
                      availabilityZones: [ availabilityZone ],
                      launchConfigurationName: launchConfigName
                  ))
                  Closure dcclo
                  cleanupTasks.add dcclo = { 
                    try {
                      setDesiredCapacity( new SetDesiredCapacityRequest( 
                          autoScalingGroupName: groupName,
                          desiredCapacity: 0
                      ) )
                      Closure asgclo
                      cleanupTasks.add asgclo = {
                        try {
                          deleteAutoScalingGroup( new DeleteAutoScalingGroupRequest(
                              autoScalingGroupName: groupName,
                          ))
                        } catch ( AmazonServiceException e ) {
                          if ( 'ScalingActivityInProgress' == e.errorCode || 'ResourceInUse' == e.errorCode) {
                            print("Rescheduling cleanup for group ${groupName}")
                            cleanupTasks.add(asgclo)
                          } else {
                            throw e
                          }
                        }
                      }
                    } catch ( AmazonServiceException e ) {
                      if ( 'ScalingActivityInProgress' == e.errorCode ) {
                        print( "Rescheduling set desired capacity for group ${groupName}" )
                        cleanupTasks.add( dcclo )
                      } else {
                        throw e
                      }
                    }
                  }
                  if (count % 100 == 0) {
                    println("[${thread}] Created ${count} groups")
                  }
                }
              }
            }
          } finally {
            latch.countDown( )
          }
        }
      }
      latch.await( )

      print( "Created ${accounts*groups} auto scaling group(s) of ${instancesPerGroup} instance(s), describing." )
      long before = System.currentTimeMillis( )
      getASClient( ).with {
        describeAutoScalingGroups( new DescribeAutoScalingGroupsRequest(
            autoScalingGroupNames: [ 'verbose' ]
        ) ).with {
          print( "Described ${autoScalingGroups.size()} auto scaling groups" )
        }
      }
      print( "Described auto scaling groups in ${System.currentTimeMillis()-before}ms" )

      print( "Waiting for instances to be InService" )
      ( 1..200 ).find{ Integer index ->
        sleep( 10000 )
        Integer inService = getASClient( ).with {
          describeAutoScalingGroups( new DescribeAutoScalingGroupsRequest(
              autoScalingGroupNames: [ 'verbose' ]
          ) ).with {
            autoScalingGroups?.inject( 0 ) { Integer count, AutoScalingGroup group ->
              count + group.instances?.inject( 0 ) { Integer instanceCount, Instance instance ->
                instanceCount + ( instance.lifecycleState == 'InService' ?  1 : 0 )
              }  
            }
          }
        }
        print( "Found ${inService} InService instances after ${10*index} seconds" )
        inService >= accounts*groups*instancesPerGroup ? inService : null
      }
      
      print( "Test complete in ${System.currentTimeMillis()-startTime}ms" )
    } finally {
      // Attempt to clean up anything we created
      print( "Running cleanup tasks" )
      final long cleanupStart = System.currentTimeMillis( )
      final CountDownLatch cleanupLatch = new CountDownLatch( allCleanupTasks.size( ) )
      allCleanupTasks.each { LinkedBlockingQueue<Runnable> cleanupTasks ->
        Thread.start {
          try {
            Closure cleanClo = { Runnable cleanupTask ->
              try {
                cleanupTask.run()
              } catch ( Exception e ) {
                e.printStackTrace( )
              }
            }
            List<Runnable> firstPass = []
            cleanupTasks.drainTo( firstPass )
            firstPass.reverse( ).each( cleanClo )
            Runnable toRun = cleanupTasks.poll( )
            int counter = 0
            while( toRun ) {
              cleanClo.call( toRun )
              toRun = cleanupTasks.poll( )
              if ( counter % 10 == 0 ) sleep( 50 )
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
