/*
 * Copyright 2018 Steve Jones. All Rights Reserved.
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
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.handlers.RequestHandler2
import com.amazonaws.services.ec2.model.Address
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest
import com.amazonaws.services.ec2.model.DescribeAddressesRequest
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult
import com.amazonaws.services.ec2.model.DescribeImagesRequest
import com.amazonaws.services.ec2.model.DescribeInstancesRequest
import com.amazonaws.services.ec2.model.Filter
import com.amazonaws.services.ec2.model.IpPermission
import com.amazonaws.services.ec2.model.RunInstancesRequest
import com.amazonaws.services.ec2.model.TerminateInstancesRequest
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancing
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient
import com.amazonaws.services.elasticloadbalancing.model.ConfigureHealthCheckRequest
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest
import com.amazonaws.services.elasticloadbalancing.model.DeleteLoadBalancerRequest
import com.amazonaws.services.elasticloadbalancing.model.DescribeInstanceHealthRequest
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersRequest
import com.amazonaws.services.elasticloadbalancing.model.HealthCheck
import com.amazonaws.services.elasticloadbalancing.model.Instance
import com.amazonaws.services.elasticloadbalancing.model.Listener
import com.amazonaws.services.elasticloadbalancing.model.RegisterInstancesWithLoadBalancerRequest
import com.amazonaws.services.identitymanagement.model.CreateAccessKeyRequest
import com.github.sjones4.youcan.youare.YouAreClient
import com.github.sjones4.youcan.youare.model.CreateAccountRequest
import com.github.sjones4.youcan.youare.model.DeleteAccountRequest
import com.github.sjones4.youcan.youprop.YouProp
import com.github.sjones4.youcan.youprop.YouPropClient
import com.github.sjones4.youcan.youprop.model.DescribePropertiesRequest
import com.github.sjones4.youcan.youserv.YouServ
import com.github.sjones4.youcan.youserv.YouServClient
import com.github.sjones4.youcan.youserv.model.DescribeServicesRequest
import com.github.sjones4.youcan.youtwo.YouTwo
import com.github.sjones4.youcan.youtwo.YouTwoClient
import com.github.sjones4.youcan.youtwo.model.DescribeInstanceTypesRequest
import org.junit.Assert
import org.junit.Test

import javax.naming.Context
import javax.naming.directory.Attributes
import javax.naming.directory.DirContext
import javax.naming.directory.InitialDirContext
import java.nio.charset.StandardCharsets
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 *
 */
class ScaleELBChurnTest {

  private final AWSCredentialsProvider eucalyptusCredentials

  ScaleELBChurnTest( ) {
    this.eucalyptusCredentials = new AWSStaticCredentialsProvider( new BasicAWSCredentials(
        Objects.toString( System.getenv('AWS_ACCESS_KEY_ID'),     System.getenv('AWS_ACCESS_KEY') ),
        Objects.toString( System.getenv('AWS_SECRET_ACCESS_KEY'), System.getenv('AWS_SECRET_KEY') )
    ) )
  }

  private String cloudUri( String env ) {
    String url = System.getenv( env )
    Assert.assertNotNull( "Expected URL from environment (${env})", url )
    URI.create( url ).toString()
  }

  private YouTwo getEC2Client( final AWSCredentialsProvider credentials = eucalyptusCredentials ) {
    final YouTwo ec2 = new YouTwoClient( credentials, new ClientConfiguration(
        socketTimeout: TimeUnit.MINUTES.toMillis( 2 )
    ) )
    ec2.setEndpoint( cloudUri( 'EC2_URL' ) )
    ec2
  }


  private AmazonElasticLoadBalancing getELBClient(final AWSCredentialsProvider credentials ) {
    final AmazonElasticLoadBalancing elb = new AmazonElasticLoadBalancingClient( credentials, new ClientConfiguration(
        socketTimeout: TimeUnit.MINUTES.toMillis( 2 )
    )  )
    elb.setEndpoint( cloudUri( 'AWS_ELB_URL' ) )
    elb
  }

  private YouAreClient getYouAreClient( final AWSCredentialsProvider credentials = eucalyptusCredentials ) {
    final YouAreClient euare = new YouAreClient( credentials, new ClientConfiguration(
        socketTimeout: TimeUnit.MINUTES.toMillis( 2 )
    ) )
    euare.setEndpoint( cloudUri( 'AWS_IAM_URL' ) )
    euare
  }

  private YouProp getPropertiesClient( final AWSCredentialsProvider credentials = eucalyptusCredentials ) {
    YouPropClient youProp = new YouPropClient( credentials )
    youProp.setEndpoint( cloudUri( 'EUCA_PROPERTIES_URL' ) )
    youProp
  }

  private YouServ getServicesClient( final AWSCredentialsProvider credentials = eucalyptusCredentials ) {
    YouServClient youServ = new YouServClient( credentials )
    youServ.setEndpoint( cloudUri( 'EUCA_BOOTSTRAP_URL' ) )
    youServ
  }

  private Set<String> getDnsHosts( final YouServ youServ ) {
    youServ.describeServices( new DescribeServicesRequest(
        filters: [
            new com.github.sjones4.youcan.youserv.model.Filter(
                name: 'service-type',
                values: [ 'dns' ]
            )
        ]
    ) ).with{
      serviceStatuses.collect{ serviceStatus ->
        URI.create( serviceStatus.serviceId.uri ).host
      } as Set<String>
    }
  }

  private String lookup( String name, Set<String> dnsServers ) {
    final Hashtable<String,String> env = new Hashtable<>()
    env.put( Context.INITIAL_CONTEXT_FACTORY, 'com.sun.jndi.dns.DnsContextFactory' )
    env.put( Context.PROVIDER_URL, dnsServers.collect{ ip -> "dns://${ip}/" }.join( ' ' ) )
    env.put( Context.AUTHORITATIVE, 'true' )
    final DirContext ictx = new InitialDirContext( env )
    try {
      final Attributes attrs = ictx.getAttributes( name, ['A'] as String[] )
      final String ip = attrs.get('a')?.get( )
      return ip
    } finally {
      ictx.close()
    }
  }

  private void print( String text ) {
    System.out.println( text )
  }

  @Test
  void test( ) {
    final YouTwo ec2 = getEC2Client( )

    // Find an AZ to use
    final DescribeAvailabilityZonesResult azResult = ec2.describeAvailabilityZones()

    Assert.assertTrue( 'Availability zone not found', azResult.getAvailabilityZones().size() > 0 )

    String availabilityZone = azResult.getAvailabilityZones().get( 0 ).getZoneName()

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
    Assert.assertNotNull( 'Image not found', imageId )
    print( "Using image: ${imageId}" )

    // Find available addresses
    final int addressCount = ec2.describeAddresses( new DescribeAddressesRequest( publicIps: ['verbose' ] ) ).with {
      addresses.count { Address address ->
        'nobody' == address?.instanceId
      }
    }
    Assert.assertTrue( "Address count ${addressCount} > 10", addressCount > 10 )
    print( "Found available public addresses: ${addressCount}" )

    // Find elb instance type for use determining capacity
    String elbInstanceType = getPropertiesClient( ).with {
      describeProperties( new DescribePropertiesRequest(
          properties: [ 'services.loadbalancing.worker.instance_type' ]
      ) )?.with {
        properties?.getAt( 0 )?.getValue( )
      }
    }
    Assert.assertNotNull( 'Load balancer instance type not found', elbInstanceType )
    print( "Found elb instance type: ${elbInstanceType}" )

    // Find a key pair
    final String key = ec2.describeKeyPairs( ).with {
      keyPairs?.getAt( 0 )?.keyName
    }
    print( "Using key: ${key}" )

    // Find dns hosts
    Set<String> dnsHosts = getDnsHosts(getServicesClient())
    print( "Using dns endpoints: ${dnsHosts}" )

    final String namePrefix = UUID.randomUUID().toString().substring(0, 13) + "-"
    print( "Using resource prefix for test: " + namePrefix )

    final long startTime = System.currentTimeMillis( )
    final List<Runnable> cleanupTasks = [] as List<Runnable>
    try {
      final String accountName = "${namePrefix}account"
      final AWSCredentialsProvider accountCredentials = getYouAreClient().with {
        print("Creating test account ${accountName}")
        createAccount(new CreateAccountRequest(accountName: accountName))
        cleanupTasks.add {
          print("Deleting account: ${accountName}")
          deleteAccount(new DeleteAccountRequest(accountName: accountName, recursive: true))
        }

        // Get credentials for new account
        AWSCredentialsProvider accountCredentials = getYouAreClient().with {
          addRequestHandler(new RequestHandler2() {
            void beforeRequest(final Request<?> request) {
              request.addParameter("DelegateAccount", accountName)
            }
          })
          createAccessKey(new CreateAccessKeyRequest(userName: "admin")).with {
            accessKey?.with {
              new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretAccessKey))
            }
          }
        }
        accountCredentials
      }
      Assert.assertNotNull("Expected account credentials", accountCredentials)

      // test running an instance with an HTTP service
      print("Launching target instance")
      String instancePublicIp = null
      String instanceGroupId = null
      String instanceId = null
      getEC2Client( accountCredentials ).with {
        String instanceSecurityGroup = "${namePrefix}instance-group"
        print("Creating security group with name: ${instanceSecurityGroup}")
        instanceGroupId = createSecurityGroup(new CreateSecurityGroupRequest(
            groupName: instanceSecurityGroup,
            description: 'Test security group for instances'
        )).with {
          groupId
        }

        print("Authorizing instance security group ${instanceSecurityGroup}/${instanceGroupId}")
        authorizeSecurityGroupIngress(new AuthorizeSecurityGroupIngressRequest(
            groupId: instanceGroupId,
            ipPermissions: [
                new IpPermission(
                    ipProtocol: 'tcp',
                    fromPort: 22,
                    toPort: 22,
                    ipRanges: ['0.0.0.0/0']
                ),
                new IpPermission(
                    ipProtocol: 'tcp',
                    fromPort: 9999,
                    toPort: 9999,
                    ipRanges: ['0.0.0.0/0']
                ),
            ]
        ))

        String userDataText = '''
          #!/usr/bin/python -tt
          import SimpleHTTPServer, BaseHTTPServer

          class StaticHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
            def do_GET(self):
              self.send_response( 200 )
              self.send_header('Content-Type', 'text/plain; charset=utf-8')
              self.end_headers( )
              self.wfile.write("Hello");
              self.wfile.close( );

          BaseHTTPServer.HTTPServer( ("", 9999), StaticHandler ).serve_forever( )
          '''.stripIndent().trim()

        print("Running instance to access via load balancers")
        instanceId = runInstances(new RunInstancesRequest(
            minCount: 1,
            maxCount: 1,
            imageId: imageId,
            keyName: key,
            securityGroupIds: [instanceGroupId],
            userData: Base64.encoder.encodeToString(userDataText.getBytes(StandardCharsets.UTF_8))
        )).with {
          reservation?.with {
            instances?.getAt(0)?.instanceId
          }
        }

        print("Instance running with identifier ${instanceId}")
        cleanupTasks.add {
          print("Terminating instance ${instanceId}")
          terminateInstances(new TerminateInstancesRequest(instanceIds: [instanceId]))

          print("Waiting for instance ${instanceId} to terminate")
          (1..25).find {
            sleep 5000
            print("Waiting for instance ${instanceId} to terminate, waited ${it * 5}s")
            describeInstances(new DescribeInstancesRequest(
                instanceIds: [instanceId],
                filters: [new Filter(name: "instance-state-name", values: ["terminated"])]
            )).with {
              reservations?.getAt(0)?.instances?.getAt(0)?.instanceId == instanceId
            }
          }
        }

        print("Waiting for instance ${instanceId} to start")
        (1..25).find {
          sleep 5000
          print("Waiting for instance ${instanceId} to start, waited ${it * 5}s")
          describeInstances(new DescribeInstancesRequest(
              instanceIds: [instanceId],
              filters: [new Filter(name: "instance-state-name", values: ["running"])]
          )).with {
            instancePublicIp = reservations?.getAt(0)?.instances?.getAt(0)?.publicIpAddress
            reservations?.getAt(0)?.instances?.getAt(0)?.instanceId == instanceId
          }
        }
        Assert.assertTrue("Expected instance public ip", instancePublicIp != null)
      }

      // Find elb instance type capacity
      Integer availability = ec2.describeInstanceTypes( new DescribeInstanceTypesRequest(
          availability: true,
          instanceTypes: [ elbInstanceType ]
      ) ).with {
        instanceTypes?.getAt( 0 )?.getAvailability( )?.find{ it.zoneName == availabilityZone }?.getAvailable( )
      }
      Assert.assertNotNull( 'Availability not found', availability )
      print( "Using instance type ${elbInstanceType} and availability zone ${availabilityZone} with availability ${availability}" )


      final int elbIterations = 5
      final int elbThreads = Math.min( 20, Math.min( availability - 1, addressCount -1 ) )
      final CountDownLatch latch = new CountDownLatch( elbThreads )
      final AtomicInteger successCount = new AtomicInteger(0)
      print( "Churning ${elbIterations} load balancers on ${elbThreads} thread(s)" )
      ( 1..elbThreads ).each { Integer thread ->
        Thread.start {
          try {
            getELBClient(accountCredentials).with {
              ( 1..elbIterations ).each { Integer count ->
                String loadBalancerName = "${namePrefix}balancer-${thread}-${count}"
                print("[${thread}] Creating load balancer ${count}/${elbIterations}: ${loadBalancerName}")
                for( int i=0; i<12; i++ ) {
                  try {
                    createLoadBalancer(new CreateLoadBalancerRequest(
                        loadBalancerName: loadBalancerName,
                        listeners: [new Listener(
                            loadBalancerPort: 9999,
                            protocol: 'HTTP',
                            instancePort: 9999,
                            instanceProtocol: 'HTTP'
                        )],
                        availabilityZones: [availabilityZone]
                    ))
                    break
                  } catch( e ) {
                    if ( e.message.contains( 'Not enough resources' ) ) {
                      print("[${thread}] Insufficient resources, will retry in 5s creating load balancer ${count}/${elbIterations}: ${loadBalancerName}")
                      sleep 5000
                    } else {
                      throw e
                    }
                  }
                }

                try {
                  String balancerHost = describeLoadBalancers(new DescribeLoadBalancersRequest(loadBalancerNames: [loadBalancerName])).with {
                    loadBalancerDescriptions.get(0).with {
                      DNSName
                    }
                  }

                  print("[${thread}] Configuring health checks for load balancer ${loadBalancerName}/${balancerHost}")
                  configureHealthCheck( new ConfigureHealthCheckRequest(
                    loadBalancerName: loadBalancerName,
                      healthCheck: new HealthCheck(
                          target: 'HTTP:9999/',
                          healthyThreshold: 2,
                          unhealthyThreshold: 6,
                          interval: 10,
                          timeout: 5
                      )
                  ))

                  print("[${thread}] Registering instance ${instanceId} with load balancer ${loadBalancerName}/${balancerHost}")
                  registerInstancesWithLoadBalancer(new RegisterInstancesWithLoadBalancerRequest(
                      loadBalancerName: loadBalancerName,
                      instances: [new Instance(instanceId)]
                  ))

                  print("[${thread}] Waiting for load balancer ${count}/${elbIterations} instance ${instanceId} to be healthy")
                  (1..60).find {
                    sleep 15000
                    print("[${thread}] Waiting for load balancer ${count}/${elbIterations} instance ${instanceId} to be healthy, waited ${it * 15}s")
                    describeInstanceHealth(new DescribeInstanceHealthRequest(
                        loadBalancerName: loadBalancerName,
                        instances: [new Instance(instanceId)]
                    )).with {
                      'InService' == instanceStates?.getAt(0)?.state
                    }
                  }

                  String instanceUrl = "http://${instancePublicIp}:9999/"
                  print("[${thread}] Accessing instance ${instanceId} ${instanceUrl}")
                  String instanceResponse = new URL(instanceUrl).
                      getText(connectTimeout: 10000, readTimeout: 10000, useCaches: false, allowUserInteraction: false)
                  Assert.assertTrue("[${thread}] Expected instance ${instanceId} response Hello, but was: ${instanceResponse}", 'Hello' == instanceResponse)
                  print("[${thread}] Response from instance ${instanceId} verified")

                  print("[${thread}] Resolving load balancer ${count}/${elbIterations} host ${balancerHost}")
                  String balancerIp = null
                  (1..12).find {
                    if (it > 1) sleep 5000
                    balancerIp = lookup(balancerHost, dnsHosts)
                  }
                  Assert.assertNotNull("[${thread}] Expected ip for load balancer ${count}/${elbIterations}", balancerIp)
                  print("[${thread}] Resolved load balancer ${count}/${elbIterations} host ${balancerHost} to ${balancerIp}")
                  String balancerUrl = "http://${balancerIp}:9999/"
                  print("[${thread}] Accessing instance ${instanceId} via load balancer ${count}/${elbIterations} ${balancerUrl}")
                  for ( int i=0; i<12; i++ ) {
                    try {
                      String balancerResponse = new URL(balancerUrl).
                          getText(connectTimeout: 10000, readTimeout: 10000, useCaches: false, allowUserInteraction: false)
                      Assert.assertTrue("[${thread}] Expected balancer ${count}/${elbIterations} response Hello, but was: ${balancerResponse}", 'Hello' == balancerResponse)
                      print("[${thread}] Response from load balancer ${count}/${elbIterations} host ${balancerHost} verified")
                      break
                    } catch ( e ) {
                      if ( e.message.contains( '503' ) ) { // check for 503 http status code and retry
                        print("[${thread}] Service unavailable, will retry in 5s accessing instance ${instanceId} via load balancer ${count}/${elbIterations} ${balancerUrl}")
                        sleep 5000
                      } else {
                        throw e
                      }
                    }
                  }
                } finally {
                  print("[${thread}] Deleting load balancer ${count}/${elbIterations} ${loadBalancerName}")
                  deleteLoadBalancer(new DeleteLoadBalancerRequest(loadBalancerName: loadBalancerName))
                }
              }
            }

            successCount.incrementAndGet( )
          } finally {
            latch.countDown( )
          }
        }
      }
      latch.await( )
      Assert.assertEquals( "All threads successful", elbThreads, successCount.get( ) )

      print( "Test complete in ${System.currentTimeMillis()-startTime}ms" )
    } finally {
      // Attempt to clean up anything we created
      print( "Running cleanup tasks" )
      final long cleanupStart = System.currentTimeMillis( )
      cleanupTasks.reverseEach { Runnable cleanupTask ->
        try {
          cleanupTask.run()
        } catch ( Exception e ) {
          e.printStackTrace( )
        }
      }
      print( "Completed cleanup tasks in ${System.currentTimeMillis()-cleanupStart}ms" )
    }
  }
}
