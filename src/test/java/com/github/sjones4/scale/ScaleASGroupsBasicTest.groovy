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

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.services.autoscaling.AmazonAutoScaling
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient
import com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupRequest
import com.amazonaws.services.autoscaling.model.CreateLaunchConfigurationRequest
import com.amazonaws.services.autoscaling.model.DeleteAutoScalingGroupRequest
import com.amazonaws.services.autoscaling.model.DeleteLaunchConfigurationRequest
import com.amazonaws.services.ec2.AmazonEC2
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult
import com.amazonaws.services.ec2.model.DescribeImagesRequest
import com.amazonaws.services.ec2.model.Filter
import org.junit.Test

import java.util.concurrent.CountDownLatch

import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue

/**
 * Scale test for auto scaling groups in a single account.
 */
class ScaleASGroupsBasicTest {

  private final AWSCredentialsProvider eucalyptusCredentials

  ScaleASGroupsBasicTest( ) {
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
    final List<List<Runnable>> allCleanupTasks = new ArrayList<>( )
    try {
      final int target = 10000
      final int threads = 20
      final int iterations = target / threads
      print( "Creating ${target} auto scaling groups using ${threads} threads" )
      final CountDownLatch latch = new CountDownLatch( threads )
      ( 1..threads ).each { Integer thread ->
        final List<Runnable> cleanupTasks = [] as List<Runnable>
        allCleanupTasks << cleanupTasks
        Thread.start {
          getASClient( ).with {
            try {
              final String launchConfigName = "${namePrefix}config-${thread}"
              getASClient( ).with {
                print( "Creating launch configuration ${launchConfigName}" )
                createLaunchConfiguration(new CreateLaunchConfigurationRequest(
                    instanceType: 'm1.small',
                    imageId: imageId,
                    launchConfigurationName: launchConfigName
                ))
                cleanupTasks.add {
                  deleteLaunchConfiguration(new DeleteLaunchConfigurationRequest(
                      launchConfigurationName: launchConfigName
                  ))
                }
              }

              (1..iterations).each { Integer count ->
                final String groupName = "${namePrefix}group-${thread}-${count}"
                createAutoScalingGroup(new CreateAutoScalingGroupRequest(
                    autoScalingGroupName: groupName,
                    minSize: 0,
                    maxSize: 0,
                    availabilityZones: [ availabilityZone ],
                    launchConfigurationName: launchConfigName
                ))
                cleanupTasks.add {
                  deleteAutoScalingGroup( new DeleteAutoScalingGroupRequest(
                      autoScalingGroupName: groupName,
                      forceDelete: true
                  ))
                }
                if (count % 100 == 0) {
                  println("[${thread}] Created ${count} groups")
                }
              }
            } finally {
              latch.countDown( )
            }
          }
        }
      }
      latch.await( )

      print( "Created ${target} auto scaling groups, describing." )
      long before = System.currentTimeMillis( )
      getASClient( ).with {
        describeAutoScalingGroups( ).with {
          print( "Described ${autoScalingGroups.size()} auto scaling groups" )
        }
      }
      print( "Described auto scaling groups in ${System.currentTimeMillis()-before}ms" )

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
