/*
 * Copyright 2016 Steve Jones. All Rights Reserved.
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

import com.amazonaws.ClientConfiguration
import com.amazonaws.Request
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.handlers.AbstractRequestHandler
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.services.ec2.AmazonEC2
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.*
import com.amazonaws.services.identitymanagement.model.CreateAccessKeyRequest
import com.github.sjones4.youcan.youare.YouAreClient
import com.github.sjones4.youcan.youare.model.CreateAccountRequest
import com.github.sjones4.youcan.youare.model.DeleteAccountRequest
import org.junit.Test

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue

/**
 * Scale test for EC2 instances in VPCs across multiple accounts.
 *
 * 500 accounts                                            [500]
 *   5 vpc(s_ per account                                [2,500]
 *   1 internet gateway(s) per vpc                       [2,500]
 *   4 subnet(s) per vpc                                [10,000]
 *   1 instance(s) per subnet                           [10,000]
 *   2 network interfaces per instance (in distinct sg) [20,000]
 *   1 security group(s) per network interface          [20,000]
 */
class ScaleEC2VPCInstancesAccountsTest {

  private final AWSCredentialsProvider eucalyptusCredentials

  ScaleEC2VPCInstancesAccountsTest( ) {
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
    final AmazonEC2 ec2 = new AmazonEC2Client( credentials, new ClientConfiguration(
        socketTimeout: TimeUnit.MINUTES.toMillis( 2 )
    ) )
    ec2.setEndpoint( cloudUri( 'EC2_URL', '/services/compute' ) )
    ec2
  }

  private YouAreClient getYouAreClient( final AWSCredentialsProvider credentials = eucalyptusCredentials ) {
    final YouAreClient euare = new YouAreClient( credentials, new ClientConfiguration(
        socketTimeout: TimeUnit.MINUTES.toMillis( 2 )
    ) )
    euare.setEndpoint( cloudUri( 'AWS_IAM_URL', '/services/Euare' ) )
    euare
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
      final int threads = 50
      final int accounts = 500
      final int vpcsPerAccount = 5
      final int subnetsPerVpc = 4
      final int instancesPerSubnet = 20
      final int interfacesPerInstance = 1
      final int securityGroupsPerInterface = 0
      final int iterations = accounts / threads
      final int totalInstancesPerAccount = vpcsPerAccount * subnetsPerVpc * instancesPerSubnet
      print( "Creating ${totalInstancesPerAccount} instances in ${accounts} accounts using ${threads} threads" )
      final CountDownLatch latch = new CountDownLatch( threads )
      ( 1..threads ).each { Integer thread ->
        final List<Runnable> cleanupTasks = [] as List<Runnable>
        allCleanupTasks << cleanupTasks
        Thread.start {
          try {
            ( 1..iterations ).each { Integer account ->
              final String accountName = "${namePrefix}account-${thread}-${account}"
              getYouAreClient( ).with {
                // Create account for testing
                print("[${thread}] Creating account ${accountName}")
                createAccount(new CreateAccountRequest(accountName: accountName))
                cleanupTasks.add {
                  print("Deleting account: ${accountName}")
                  deleteAccount(new DeleteAccountRequest(accountName: accountName, recursive: true))
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

              getEC2Client( accountCredentials ).with {
                cleanupTasks.add {
                  print("Terminating instances for account ${accountName}")
                  describeInstanceStatus( new DescribeInstanceStatusRequest( includeAllInstances: true ) ).with {
                    instanceStatuses*.instanceId.collate( 50 ).each { List<String> instanceIds ->
                      terminateInstances( new TerminateInstancesRequest( instanceIds: instanceIds ))
                    }
                  }

                  ( 1..125 ).find{ Integer index ->
                    sleep( 10000 )
                    describeInstanceStatus( new DescribeInstanceStatusRequest(
                        includeAllInstances: true,
                        filters: [ new Filter( name: 'instance-state-name', values: [ 'shutting-down' ] ) ]
                    ) ).with {
                        instanceStatuses.isEmpty( )
                    }
                  }

                  print("Deleting security groups for account ${accountName}")
                  describeSecurityGroups( ).with {
                    securityGroups.findAll{ it.groupName != 'default' }*.groupId.each{ String groupId ->
                      deleteSecurityGroup( new DeleteSecurityGroupRequest( groupId: groupId ) )
                    }
                  }

                  print("Deleting internet gateways for account ${accountName}")
                  describeInternetGateways( ).with {
                    internetGateways*.internetGatewayId.each{ String internetGatewayId ->
                      deleteInternetGateway( new DeleteInternetGatewayRequest( internetGatewayId: internetGatewayId ) )
                    }
                  }

                  print("Deleting subnets for account ${accountName}")
                  describeSubnets( ).with {
                    subnets*.subnetId.each{ String subnetId ->
                      deleteSubnet( new DeleteSubnetRequest( subnetId: subnetId ) )
                    }
                  }

                  print("Deleting vpcs for account ${accountName}")
                  describeVpcs( ).with {
                    vpcs*.vpcId.each{ String vpcId ->
                      deleteVpc( new DeleteVpcRequest(vpcId: vpcId ) )
                    }
                  }
                }


                (1..vpcsPerAccount).each { Integer vpcIndex ->
                  final String vpcId = createVpc( new CreateVpcRequest( cidrBlock: "10.${vpcIndex}.0.0/16" ) ).with {
                    vpc?.vpcId
                  }

                  final String gatewayId = createInternetGateway( ).with { internetGateway?.internetGatewayId }

                  attachInternetGateway( new AttachInternetGatewayRequest(
                      vpcId: vpcId,
                      internetGatewayId: gatewayId
                  ) )

                  (1..subnetsPerVpc).each { Integer subnetIndex ->
                    final String subnetId = createSubnet( new CreateSubnetRequest( vpcId: vpcId, cidrBlock: "10.${vpcIndex}.${subnetIndex}.0/24" ) ).with {
                      subnet?.subnetId
                    }

                    //TODO do we want groups to be separate for every instance?
                    runInstances(new RunInstancesRequest(
                        imageId: imageId,
                        instanceType: 't1.micro',
                        networkInterfaces: (1..interfacesPerInstance).collect{ Integer interfaceIndex ->
                          new InstanceNetworkInterfaceSpecification(
                              deviceIndex: interfaceIndex - 1,
                              subnetId: subnetId,
                              groups: securityGroupsPerInterface==0 ?
                                  null :
                                  (1..securityGroupsPerInterface).collect{ Integer securityGroupIndex ->
                                createSecurityGroup( new CreateSecurityGroupRequest(
                                  groupName: "${namePrefix}${vpcIndex}-${subnetIndex}-${interfaceIndex}-${securityGroupIndex}",
                                  description: "Group for ${accountName} ${vpcIndex} ${subnetIndex} ${interfaceIndex} ${securityGroupIndex}",
                                  vpcId: vpcId
                                ) ).with{
                                  groupId
                                }
                              }
                          )
                        },
                        minCount: instancesPerSubnet,
                        maxCount: instancesPerSubnet,
                        clientToken: "${namePrefix}${thread}${vpcIndex}${subnetIndex}-${instancesPerSubnet}"
                    ))
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

      print( "Launched ${accounts*totalInstancesPerAccount} instances, describing." )
      long before = System.currentTimeMillis( )
      getEC2Client( ).with {
        describeInstances( new DescribeInstancesRequest(
          instanceIds: [ 'verbose' ],
          filters: [
              new Filter( name: 'instance-state-name', values: [ 'pending', 'running' ] ),
              new Filter( name: 'client-token', values: [ "${namePrefix}*" as String ] ),
          ]
        ) ).with {
          print( "Described ${reservations.size()} instances" )
        }
      }
      print( "Described instances in ${System.currentTimeMillis()-before}ms" )

      ( 1..125 ).find{ Integer index ->
        sleep( 10000 )
        print( "Describing instance status ${index}" )
        long beforeIS = System.currentTimeMillis( )
        Integer pending = getEC2Client( ).with {
          describeInstanceStatus( new DescribeInstanceStatusRequest(
              instanceIds: [ 'verbose' ],
              includeAllInstances: true,
              filters: [ new Filter( name: 'instance-state-name', values: [ 'pending'] ) ] )
          ).with {
            print( "Described ${instanceStatuses.size()} pending instance status" )
            instanceStatuses.size( )
          }
        }
        print( "Described instance status in ${System.currentTimeMillis()-beforeIS}ms" )
        pending == 0
      }

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
