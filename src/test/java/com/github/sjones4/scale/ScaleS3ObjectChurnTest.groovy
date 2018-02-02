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
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.handlers.AbstractRequestHandler
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.services.identitymanagement.model.CreateAccessKeyRequest
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.github.sjones4.youcan.youare.YouAreClient
import com.github.sjones4.youcan.youare.model.CreateAccountRequest
import com.github.sjones4.youcan.youare.model.DeleteAccountRequest
import com.google.common.io.ByteStreams
import org.junit.Assert
import org.junit.Test

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Scale test for S3 object churn.
 */
class ScaleS3ObjectChurnTest {

  private final AWSCredentialsProvider eucalyptusCredentials

  ScaleS3ObjectChurnTest( ) {
    this.eucalyptusCredentials = new StaticCredentialsProvider( new BasicAWSCredentials(
        Objects.toString( System.getenv('AWS_ACCESS_KEY_ID'),     System.getenv('AWS_ACCESS_KEY') ),
        Objects.toString( System.getenv('AWS_SECRET_ACCESS_KEY'), System.getenv('AWS_SECRET_KEY') )
    ) )
  }

  private String cloudUri( String env ) {
    String url = System.getenv( env )
    Assert.assertNotNull( "Expected URL from environment (${env})", url )
    URI.create( url ).toString()
  }

  private YouAreClient getYouAreClient( final AWSCredentialsProvider credentials = eucalyptusCredentials ) {
    final YouAreClient euare = new YouAreClient( credentials, new ClientConfiguration(
        socketTimeout: TimeUnit.MINUTES.toMillis( 2 )
    ) )
    euare.setEndpoint( cloudUri( 'AWS_IAM_URL' ) )
    euare
  }

  private AmazonS3 getS3Client(final AWSCredentialsProvider credentials ) {
    final AmazonS3Client s3 = new AmazonS3Client( credentials, new ClientConfiguration(
        socketTimeout: TimeUnit.MINUTES.toMillis( 2 )
    )  )
    s3.setEndpoint( cloudUri( 'S3_URL' ) )
    s3
  }

  private void print( String text ) {
    System.out.println( text )
  }

  @Test
  void test( ) {
    final String namePrefix = UUID.randomUUID().toString().substring(0, 13) + "-"
    print( "Using resource prefix for test: " + namePrefix )

    final long startTime = System.currentTimeMillis( )
    final List<Runnable> cleanupTasks = [] as List<Runnable>
    try {
      final String accountName = "${namePrefix}account"
      AWSCredentialsProvider accountCredentials = getYouAreClient( ).with {
        print("Creating test account ${accountName}")
        createAccount(new CreateAccountRequest(accountName: accountName))
        cleanupTasks.add {
          print("Deleting account: ${accountName}")
          deleteAccount(new DeleteAccountRequest(accountName: accountName, recursive: true))
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
        accountCredentials
      }
      Assert.assertNotNull("Expected account credentials", accountCredentials)

      String bucketName = "${namePrefix}bucket"
      print( "Creating bucket ${bucketName}" )
      getS3Client( accountCredentials ).with {
        createBucket(bucketName)
        cleanupTasks.add {
          print("Deleting bucket ${bucketName}")
          deleteBucket(bucketName)
        }
      }

      final List<List<Number>> churnOpts = [
          // object size,                #obj, #threads up, #down
        [ 1024 * 1024 * 10    /*    10MiB */, 75, 15, 30 ],
        [ 1024 * 1024 * 100   /*   100MiB */, 25,  5, 10 ],
        [ 1024 * 1024 * 1000  /*  1000MiB */, 10,  2,  5 ],
        [ 1024 * 1024 * 10000 /* 10000MiB */,  2,  2,  5 ],
      ]
      churnOpts.each { long size, int objects, int uploadThreads, int downloadThreads ->
        final int threads = uploadThreads
        final int iterations = objects / threads

        print("Churning ${iterations} object put/get/delete on ${threads} threads")
        final CountDownLatch latch = new CountDownLatch(threads)
        final AtomicInteger successCount = new AtomicInteger(0)
        final byte[] data = new byte[size]
        (1..threads).each { Integer thread ->
          Thread.start {
            try {
              getS3Client( accountCredentials ).with {
                for (int i = 0; i < iterations; i++) {
                  String key = "${namePrefix}object-${thread}-${i}"
                  print("[${thread}] putting object ${key} length ${data.length}")
                  putObject(new PutObjectRequest(
                      bucketName,
                      key,
                      new ByteArrayInputStream(data),
                      new ObjectMetadata(contentLength: data.length)
                  ))

                  print("[${thread}] getting object ${key}")
                  ByteStreams.copy(getObject(bucketName, key).getObjectContent(), ByteStreams.nullOutputStream())

                  print("[${thread}] deleting object ${key}")
                  deleteObject(bucketName, key)
                }
                successCount.incrementAndGet()
              }
            } finally {
              latch.countDown()
            }
          }
        }
        latch.await()
        Assert.assertEquals( "Threads completed", threads, successCount.get( ) )

        int downloadCount = (objects * 10) / downloadThreads
        print("Churning ${downloadCount} object gets on ${downloadThreads} threads")
        final String key = "${namePrefix}object"
        print("Putting object ${key} length ${data.length}")
        getS3Client( accountCredentials ).with {
          putObject(new PutObjectRequest(
              bucketName,
              key,
              new ByteArrayInputStream(data),
              new ObjectMetadata(contentLength: data.length)
          ))

        }
        final CountDownLatch downLatch = new CountDownLatch(downloadThreads)
        final AtomicInteger downSuccessCount = new AtomicInteger(0)
        (1..downloadThreads).each { Integer thread ->
          Thread.start {
            try {
              getS3Client( accountCredentials ).with {
                for (int i = 0; i < downloadCount; i++) {
                  print("[${thread}] getting object ${key} ${i}/${downloadCount}")
                  try {
                    ByteStreams.copy(getObject(bucketName, key).getObjectContent(), ByteStreams.nullOutputStream())
                  } catch( e ) {
                    print( "[${thread}] Error getting object  ${key} ${i}/${downloadCount}: ${e}" )
                    Assert.fail( "error" )
                  }
                }
                downSuccessCount.incrementAndGet()
              }
            } finally {
              downLatch.countDown()
            }
          }
        }
        downLatch.await()
        getS3Client( accountCredentials ).with {
          print( "Deleting object ${key}" )
          deleteObject( bucketName, key )
        }
        Assert.assertEquals( "Download threads completed", downloadThreads, downSuccessCount.get( ) )
      }

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
