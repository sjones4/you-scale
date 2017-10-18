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

import com.amazonaws.ClientConfiguration
import com.amazonaws.Request
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.handlers.AbstractRequestHandler
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.services.cloudformation.AmazonCloudFormationClient
import com.amazonaws.services.cloudformation.model.CreateStackRequest
import com.amazonaws.services.cloudformation.model.DeleteStackRequest
import com.amazonaws.services.cloudformation.model.DescribeStacksRequest
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
import static org.junit.Assert.fail

/**
 * Scale test for EC2 instances in multiple accounts.
 *
 * https://eucalyptus.atlassian.net/browse/EUCA-11095
 */
class ScaleCFStacksAccountsTest {

  private final AWSCredentialsProvider eucalyptusCredentials

  private final String TEMPLATE = '''\
  {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Description": "Scale test stack with 10 security groups and rules",
    "Resources": {
      "ServerSecurityGroup1": {
        "Type": "AWS::EC2::SecurityGroup",
        "Properties": {
          "GroupDescription": " scale test group",
          "SecurityGroupIngress": [
            {
              "IpProtocol": "tcp",
              "FromPort": "80",
              "ToPort": "80",
              "CidrIp": "0.0.0.0/0"
            },
            {
              "IpProtocol": "tcp",
              "FromPort": "22",
              "ToPort": "22",
              "CidrIp": "192.168.1.1/32"
            }
          ]
        }
      },
      "ServerSecurityGroup2": {
        "Type": "AWS::EC2::SecurityGroup",
        "Properties": {
          "GroupDescription": " scale test group",
          "SecurityGroupIngress": [
            {
              "IpProtocol": "tcp",
              "FromPort": "80",
              "ToPort": "80",
              "CidrIp": "0.0.0.0/0"
            },
            {
              "IpProtocol": "tcp",
              "FromPort": "22",
              "ToPort": "22",
              "CidrIp": "192.168.1.1/32"
            }
          ]
        }
      },
      "ServerSecurityGroup3": {
        "Type": "AWS::EC2::SecurityGroup",
        "Properties": {
          "GroupDescription": " scale test group",
          "SecurityGroupIngress": [
            {
              "IpProtocol": "tcp",
              "FromPort": "80",
              "ToPort": "80",
              "CidrIp": "0.0.0.0/0"
            },
            {
              "IpProtocol": "tcp",
              "FromPort": "22",
              "ToPort": "22",
              "CidrIp": "192.168.1.1/32"
            }
          ]
        }
      },
      "ServerSecurityGroup4": {
        "Type": "AWS::EC2::SecurityGroup",
        "Properties": {
          "GroupDescription": " scale test group",
          "SecurityGroupIngress": [
            {
              "IpProtocol": "tcp",
              "FromPort": "80",
              "ToPort": "80",
              "CidrIp": "0.0.0.0/0"
            },
            {
              "IpProtocol": "tcp",
              "FromPort": "22",
              "ToPort": "22",
              "CidrIp": "192.168.1.1/32"
            }
          ]
        }
      },
      "ServerSecurityGroup5": {
        "Type": "AWS::EC2::SecurityGroup",
        "Properties": {
          "GroupDescription": " scale test group",
          "SecurityGroupIngress": [
            {
              "IpProtocol": "tcp",
              "FromPort": "80",
              "ToPort": "80",
              "CidrIp": "0.0.0.0/0"
            },
            {
              "IpProtocol": "tcp",
              "FromPort": "22",
              "ToPort": "22",
              "CidrIp": "192.168.1.1/32"
            }
          ]
        }
      },
      "ServerSecurityGroup6": {
        "Type": "AWS::EC2::SecurityGroup",
        "Properties": {
          "GroupDescription": " scale test group",
          "SecurityGroupIngress": [
            {
              "IpProtocol": "tcp",
              "FromPort": "80",
              "ToPort": "80",
              "CidrIp": "0.0.0.0/0"
            },
            {
              "IpProtocol": "tcp",
              "FromPort": "22",
              "ToPort": "22",
              "CidrIp": "192.168.1.1/32"
            }
          ]
        }
      },
      "ServerSecurityGroup7": {
        "Type": "AWS::EC2::SecurityGroup",
        "Properties": {
          "GroupDescription": " scale test group",
          "SecurityGroupIngress": [
            {
              "IpProtocol": "tcp",
              "FromPort": "80",
              "ToPort": "80",
              "CidrIp": "0.0.0.0/0"
            },
            {
              "IpProtocol": "tcp",
              "FromPort": "22",
              "ToPort": "22",
              "CidrIp": "192.168.1.1/32"
            }
          ]
        }
      },
      "ServerSecurityGroup8": {
        "Type": "AWS::EC2::SecurityGroup",
        "Properties": {
          "GroupDescription": " scale test group",
          "SecurityGroupIngress": [
            {
              "IpProtocol": "tcp",
              "FromPort": "80",
              "ToPort": "80",
              "CidrIp": "0.0.0.0/0"
            },
            {
              "IpProtocol": "tcp",
              "FromPort": "22",
              "ToPort": "22",
              "CidrIp": "192.168.1.1/32"
            }
          ]
        }
      },
      "ServerSecurityGroup9": {
        "Type": "AWS::EC2::SecurityGroup",
        "Properties": {
          "GroupDescription": " scale test group",
          "SecurityGroupIngress": [
            {
              "IpProtocol": "tcp",
              "FromPort": "80",
              "ToPort": "80",
              "CidrIp": "0.0.0.0/0"
            },
            {
              "IpProtocol": "tcp",
              "FromPort": "22",
              "ToPort": "22",
              "CidrIp": "192.168.1.1/32"
            }
          ]
        }
      },
      "ServerSecurityGroup10": {
        "Type": "AWS::EC2::SecurityGroup",
        "Properties": {
          "GroupDescription": " scale test group",
          "SecurityGroupIngress": [
            {
              "IpProtocol": "tcp",
              "FromPort": "80",
              "ToPort": "80",
              "CidrIp": "0.0.0.0/0"
            },
            {
              "IpProtocol": "tcp",
              "FromPort": "22",
              "ToPort": "22",
              "CidrIp": "192.168.1.1/32"
            }
          ]
        }
      }
    }
  }
  '''.stripIndent( )

  ScaleCFStacksAccountsTest( ) {
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

  private AmazonCloudFormationClient getCloudFormationClient(final AWSCredentialsProvider credentials = eucalyptusCredentials ) {
    final AmazonCloudFormationClient cf = new AmazonCloudFormationClient( credentials, new ClientConfiguration(
        socketTimeout: TimeUnit.MINUTES.toMillis( 2 )
    ) )
    cf.setEndpoint( cloudUri( 'AWS_CLOUDFORMATION_URL', '/services/CloudFormation' ) )
    cf
  }

  private void print( String text ) {
    System.out.println( text )
  }

  @Test
  void test( ) {
    final String namePrefix = "x-${UUID.randomUUID().toString().substring(0, 13)}-";
    print( "Using resource prefix for test: " + namePrefix );

    final long startTime = System.currentTimeMillis( )
    final List<List<Runnable>> allCleanupTasks = new ArrayList<>( )
    try {
      final int threads = 5
      final int accounts = 10
      final int stacks = 2
      final int iterations = accounts / threads
      print( "Creating ${stacks} stacks in ${accounts} accounts using ${threads} threads" )
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
                  void beforeRequest(final Request<?> request) {
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

              final AmazonCloudFormationClient cf = getCloudFormationClient( accountCredentials )
              ( 1..stacks ).each { Integer stack ->
                String stackName = "${namePrefix}stack-${thread}-${stack}"
                print( "[${thread}] Creating stack ${stackName} ${stack}/${stacks}" )
                cf.createStack(new CreateStackRequest(stackName: stackName, templateBody: TEMPLATE))
                (1..100).find { Integer iter ->
                  sleep(5000)
                  print("[${thread}] Waiting for stack ${stackName} to be created (${5 * iter}s)")
                  String stackStatus = cf.describeStacks(new DescribeStacksRequest(stackName: stackName)).with { res ->
                    res?.stacks?.getAt(0)?.stackStatus
                  }
                  if (stackStatus == 'CREATE_COMPLETE') {
                    stackStatus
                  } else if (stackStatus == 'CREATE_IN_PROGRESS') {
                    null
                  } else {
                    fail("Unexpected stack ${stackName} staus ${stackStatus}")
                  }
                }

                print("[${thread}] Deleting stack ${stackName} ${stack}/${stacks}")
                cf.deleteStack(new DeleteStackRequest(stackName: stackName))
                (1..100).find { Integer iter ->
                  sleep(5000)
                  print("[${thread}] Waiting for stack ${stackName} to be deleted (${5 * iter}s)")
                  String stackStatus = cf.describeStacks(new DescribeStacksRequest(stackName: stackName)).with { res ->
                    res?.stacks?.getAt(0)?.stackStatus
                  }
                  if (stackStatus == null || stackStatus == 'DELETE_COMPLETE') {
                    'DELETE_COMPLETE'
                  } else if (stackStatus == 'DELETE_IN_PROGRESS') {
                    null
                  } else if (stackStatus == 'CREATE_COMPLETE') {
                    print("Stack delete not in progress, retrying")
                    cf.deleteStack(new DeleteStackRequest(stackName: stackName))
                    null
                  } else {
                    fail("Unexpected stack ${stackName} staus ${stackStatus}")
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
