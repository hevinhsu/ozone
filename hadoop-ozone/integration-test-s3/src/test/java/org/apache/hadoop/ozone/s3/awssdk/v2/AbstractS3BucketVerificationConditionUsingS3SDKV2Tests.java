/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3.awssdk.v2;

import static org.junit.jupiter.api.Assertions.*;
import static software.amazon.awssdk.core.sync.RequestBody.fromString;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.s3.S3ClientFactory;
import org.apache.hadoop.ozone.s3.S3GatewayConfigKeys;
import org.apache.hadoop.ozone.s3.S3GatewayService;
import org.apache.ozone.test.OzoneTestBase;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.function.Executable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutBucketAclRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/**
 * Tests the S3 Bucket Verification Condition.
 * This class is used to verify the bucket creation and existence conditions
 * in the S3 compatibility layer of Ozone.
 * See:
 * - https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-owner-condition.html
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
public abstract class AbstractS3BucketVerificationConditionUsingS3SDKV2Tests extends OzoneTestBase {

  private static MiniOzoneCluster cluster = null;
  private static S3Client s3Client = null;

  /**
   * Create a MiniOzoneCluster with S3G enabled for testing.
   *
   * @param conf Configurations to start the cluster
   * @throws Exception exception thrown when waiting for the cluster to be ready.
   */
  static void startCluster(OzoneConfiguration conf) throws Exception {
    S3GatewayService s3g = new S3GatewayService();
    conf.set(S3GatewayConfigKeys.OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED, "true");
    conf.set(OzoneConfigKeys.OZONE_S3G_DEFAULT_BUCKET_LAYOUT_KEY, BucketLayout.FILE_SYSTEM_OPTIMIZED.name());
    cluster = MiniOzoneCluster.newBuilder(conf)
        .addService(s3g)
        .setNumDatanodes(5)
        .build();
    cluster.waitForClusterToBeReady();

    S3ClientFactory s3Factory = new S3ClientFactory(s3g.getConf());
    s3Client = s3Factory.createS3ClientV2();
  }

  /**
   * Shutdown the MiniOzoneCluster.
   */
  static void shutdownCluster() throws IOException {
    if (s3Client != null) {
      s3Client.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static final String defaultBucketName = "test-bucket-verification-condition-bucket";
  private static final String wrongOwner = "wrong-owner";
  private static String correctOwner;
  private static final String testKey = "test-key";
  private static final String testContent = "hello-ozone";

  static void createDefaultResource() {
    // bucket
    s3Client.createBucket(b -> b.bucket(defaultBucketName));
    GetBucketAclRequest normalRequest = GetBucketAclRequest.builder().bucket(defaultBucketName).build();
    correctOwner = s3Client.getBucketAcl(normalRequest).owner().displayName();

    // object
    s3Client.putObject(b -> b.bucket(defaultBucketName).key(testKey), fromString(testContent));
  }

  @Nested
  class BucketEndpointTests {

    @Test
    public void testGetBucketAcl() {
      GetBucketAclRequest correctRequest = GetBucketAclRequest.builder()
          .bucket(defaultBucketName)
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.getBucketAcl(correctRequest));

      GetBucketAclRequest wrongRequest = GetBucketAclRequest.builder()
          .bucket(defaultBucketName)
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.getBucketAcl(wrongRequest));
    }

    @Test
    public void testListObject() {
      ListObjectsRequest correctRequest = ListObjectsRequest.builder()
          .bucket(defaultBucketName)
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.listObjects(correctRequest));

      ListObjectsRequest wrongRequest = ListObjectsRequest.builder()
          .bucket(defaultBucketName)
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.listObjects(wrongRequest));
    }

    @Test
    public void testListMultipartUploads() {
      ListMultipartUploadsRequest correctRequest = ListMultipartUploadsRequest.builder()
          .bucket(defaultBucketName)
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.listMultipartUploads(correctRequest));

      ListMultipartUploadsRequest wrongRequest = ListMultipartUploadsRequest.builder()
          .bucket(defaultBucketName)
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(
          () -> s3Client.listMultipartUploads(wrongRequest));
    }

    @Test
    public void testPutAcl() {
      PutBucketAclRequest correctRequest = PutBucketAclRequest.builder()
          .bucket(defaultBucketName)
          .grantRead("")
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.putBucketAcl(correctRequest));

      PutBucketAclRequest wrongRequest = PutBucketAclRequest.builder()
          .bucket(defaultBucketName)
          .grantRead("")
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.putBucketAcl(wrongRequest));
    }

    @Test
    public void testHeadBucket() {
      HeadBucketRequest correctRequest = HeadBucketRequest.builder()
          .bucket(defaultBucketName)
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.headBucket(correctRequest));

      HeadBucketRequest wrongRequest = HeadBucketRequest.builder()
          .bucket(defaultBucketName)
          .expectedBucketOwner(wrongOwner)
          .build();
      S3Exception exception = assertThrows(S3Exception.class, () -> s3Client.headBucket(wrongRequest));
      assertEquals(403, exception.statusCode());
    }

    @Test
    public void testDeleteBucket() {
      s3Client.createBucket(builder -> builder.bucket("for-delete"));
      String correctOwner = s3Client.getBucketAcl(builder -> builder.bucket("for-delete")).owner().displayName();

      DeleteBucketRequest wrongRequest = DeleteBucketRequest.builder()
          .bucket("for-delete")
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.deleteBucket(wrongRequest));

      DeleteBucketRequest correctRequest = DeleteBucketRequest.builder()
          .bucket("for-delete")
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.deleteBucket(correctRequest));
    }

    @Test
    public void testMultiDelete() {
      DeleteObjectsRequest correctRequest = DeleteObjectsRequest.builder()
          .bucket(defaultBucketName)
          .expectedBucketOwner(correctOwner)
          .delete(Delete.builder().objects(ObjectIdentifier.builder().key("test").build()).build())
          .build();

      verifyPassBucketOwnershipVerification(() -> s3Client.deleteObjects(correctRequest));

      DeleteObjectsRequest wrongRequest = DeleteObjectsRequest.builder()
          .bucket(defaultBucketName)
          .expectedBucketOwner(wrongOwner)
          .delete(Delete.builder().objects(ObjectIdentifier.builder().key("test").build()).build())
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.deleteObjects(wrongRequest));
    }
  }

  @Nested
  class ObjectEndpointTests {

    @Test
    public void testCreateKey() {
      String newKey = "create-key";

      PutObjectRequest wrongRequest = PutObjectRequest.builder()
          .bucket(defaultBucketName)
          .key(newKey)
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.putObject(wrongRequest, fromString(testContent)));

      PutObjectRequest correctRequest = PutObjectRequest.builder()
          .bucket(defaultBucketName)
          .key(newKey)
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.putObject(correctRequest, fromString(testContent)));
    }

    @Test
    public void testPutObjectTagging() {
      List<Tag> tags = ImmutableList.of(
          Tag.builder().key("env").value("test").build(),
          Tag.builder().key("project").value("example").build()
      );
      PutObjectTaggingRequest wrongRequest = PutObjectTaggingRequest.builder()
          .bucket(defaultBucketName)
          .key(testKey)
          .tagging(Tagging.builder().tagSet(tags).build())
          .expectedBucketOwner(wrongOwner)
          .build();

      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.putObjectTagging(wrongRequest));

      PutObjectTaggingRequest correctRequest = PutObjectTaggingRequest.builder()
          .bucket(defaultBucketName)
          .key(testKey)
          .tagging(Tagging.builder().tagSet(tags).build())
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.putObjectTagging(correctRequest));
    }

    @Test
    public void testCreateMultipartKey() {
      CreateMultipartUploadRequest wrongRequest = CreateMultipartUploadRequest.builder()
          .bucket(defaultBucketName)
          .key(testKey)
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.createMultipartUpload(wrongRequest));

      CreateMultipartUploadRequest correctRequest = CreateMultipartUploadRequest.builder()
          .bucket(defaultBucketName)
          .key(testKey)
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.createMultipartUpload(correctRequest));
    }

    @Test
    public void testMultipartByCopy() {
      String sourceKey = "test-multipart-by-copy-source-key";
      String destKey = "test-multipart-by-copy-dest-key";

      s3Client.putObject(b -> b.bucket(defaultBucketName).key(sourceKey), fromString(testContent));

      CreateMultipartUploadResponse initResponse =
          s3Client.createMultipartUpload(b -> b.bucket(defaultBucketName).key(destKey));

      String uploadId = initResponse.uploadId();

      UploadPartCopyRequest wrongRequest = UploadPartCopyRequest.builder()
          .sourceBucket(defaultBucketName)
          .sourceKey(sourceKey)
          .expectedSourceBucketOwner(wrongOwner)
          .destinationBucket(defaultBucketName)
          .destinationKey(destKey)
          .uploadId(uploadId)
          .partNumber(1)
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.uploadPartCopy(wrongRequest));

      UploadPartCopyRequest correctRequest = UploadPartCopyRequest.builder()
          .sourceBucket(defaultBucketName)
          .sourceKey(sourceKey)
          .expectedSourceBucketOwner(correctOwner)
          .destinationBucket(defaultBucketName)
          .destinationKey(destKey)
          .uploadId(uploadId)
          .expectedBucketOwner(correctOwner)
          .partNumber(1)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.uploadPartCopy(correctRequest));
    }

    @Test
    public void testCopyObject() {
      String sourceKey = "test-copy-object-source-key";
      String destKey = "test-copy-object-dest-key";
      s3Client.putObject(b -> b.bucket(defaultBucketName).key(sourceKey), fromString("test"));

      CopyObjectRequest wrongRequest = CopyObjectRequest.builder()
          .sourceBucket(defaultBucketName)
          .sourceKey(sourceKey)
          .destinationBucket(defaultBucketName)
          .destinationKey(destKey)
          .expectedSourceBucketOwner(wrongOwner)
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.copyObject(wrongRequest));

      CopyObjectRequest correctRequest = CopyObjectRequest.builder()
          .sourceBucket(defaultBucketName)
          .sourceKey(sourceKey)
          .destinationBucket(defaultBucketName)
          .destinationKey(destKey)
          .expectedSourceBucketOwner(correctOwner)
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.copyObject(correctRequest));
    }

    @Test
    public void testCreateDirectory() {
      String newKey = "create-directory-key/";

      PutObjectRequest wrongRequest = PutObjectRequest.builder()
          .bucket(defaultBucketName)
          .key(newKey)
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.putObject(wrongRequest, RequestBody.empty()));

      PutObjectRequest correctRequest = PutObjectRequest.builder()
          .bucket(defaultBucketName)
          .key(newKey)
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.putObject(correctRequest, RequestBody.empty()));
    }

    @Test
    public void testGetKey() {
      GetObjectRequest correctRequest = GetObjectRequest.builder()
          .bucket(defaultBucketName)
          .key(testKey)
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.getObject(correctRequest));

      GetObjectRequest wrongRequest = GetObjectRequest.builder()
          .bucket(defaultBucketName)
          .key(testKey)
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.getObject(wrongRequest));
    }

    @Test
    public void testGetObjectTagging() {
      GetObjectTaggingRequest correctRequest = GetObjectTaggingRequest.builder()
          .bucket(defaultBucketName)
          .key(testKey)
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.getObjectTagging(correctRequest));

      GetObjectTaggingRequest wrongRequest = GetObjectTaggingRequest.builder()
          .bucket(defaultBucketName)
          .key(testKey)
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.getObjectTagging(wrongRequest));
    }

    @Test
    public void testListParts() {
      String newKey = "list-parts-key";
      CreateMultipartUploadResponse multipartUploadResponse = s3Client.createMultipartUpload(b -> {
        b.bucket(defaultBucketName)
            .key(newKey)
            .build();
      });

      String uploadId = multipartUploadResponse.uploadId();

      UploadPartResponse uploadPartResponse = s3Client.uploadPart(
          UploadPartRequest.builder()
              .bucket(defaultBucketName)
              .key(newKey)
              .uploadId(uploadId)
              .partNumber(1)
              .contentLength((long) testContent.getBytes(StandardCharsets.UTF_8).length)
              .build(), fromString(testContent));

      ListPartsRequest correctRequest = ListPartsRequest.builder()
          .bucket(defaultBucketName)
          .key(newKey)
          .uploadId(uploadId)
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.listParts(correctRequest));

      ListPartsRequest wrongResponse = ListPartsRequest.builder()
          .bucket(defaultBucketName)
          .key(newKey)
          .uploadId(uploadId)
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.listParts(wrongResponse));
    }

    @Test
    public void testHeadKey() {
      HeadObjectRequest correctRequest = HeadObjectRequest.builder()
          .bucket(defaultBucketName)
          .key(testKey)
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.headObject(correctRequest));

      HeadObjectRequest wrongRequest = HeadObjectRequest.builder()
          .bucket(defaultBucketName)
          .key(testKey)
          .expectedBucketOwner(wrongOwner)
          .build();
      S3Exception exception = assertThrows(S3Exception.class, () -> s3Client.headObject(wrongRequest));
      assertEquals(403, exception.statusCode());
    }

    @Test
    public void testDeleteKey() {
      String newKey = "delete-key";
      s3Client.putObject(b -> b.bucket(defaultBucketName).key(newKey), fromString(testContent));

      DeleteObjectsRequest wrongRequest = DeleteObjectsRequest.builder()
          .bucket(defaultBucketName)
          .expectedBucketOwner(wrongOwner)
          .delete(Delete.builder().objects(ObjectIdentifier.builder().key(newKey).build()).build())
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.deleteObjects(wrongRequest));

      DeleteObjectsRequest correctRequest = DeleteObjectsRequest.builder()
          .bucket(defaultBucketName)
          .expectedBucketOwner(correctOwner)
          .delete(Delete.builder().objects(ObjectIdentifier.builder().key(newKey).build()).build())
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.deleteObjects(correctRequest));
    }

    @Test
    public void testDeleteObjectTagging() {
      DeleteObjectTaggingRequest correctRequest = DeleteObjectTaggingRequest.builder()
          .bucket(defaultBucketName)
          .key(testKey)
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.deleteObjectTagging(correctRequest));

      DeleteObjectTaggingRequest wrongRequest = DeleteObjectTaggingRequest.builder()
          .bucket(defaultBucketName)
          .key(testKey)
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.deleteObjectTagging(wrongRequest));
    }

    @Test
    public void testAbortMultipartUpload() {
      CreateMultipartUploadResponse multipartUploadResponse =
          s3Client.createMultipartUpload(b -> b.bucket(defaultBucketName).key(testKey));

      String uploadId = multipartUploadResponse.uploadId();

      AbortMultipartUploadRequest wrongRequest = AbortMultipartUploadRequest.builder()
          .bucket(defaultBucketName)
          .key(testKey)
          .uploadId(uploadId)
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.abortMultipartUpload(wrongRequest));

      AbortMultipartUploadRequest correctRequest = AbortMultipartUploadRequest.builder()
          .bucket(defaultBucketName)
          .key(testKey)
          .uploadId(uploadId)
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.abortMultipartUpload(correctRequest));
    }

    @Test
    public void testInitMultipartUpload() {
      String newKey = "init-multipart-upload-key";

      CreateMultipartUploadRequest wrongRequest = CreateMultipartUploadRequest.builder()
          .bucket(defaultBucketName)
          .key(newKey)
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.createMultipartUpload(wrongRequest));

      CreateMultipartUploadRequest correctRequest = CreateMultipartUploadRequest.builder()
          .bucket(defaultBucketName)
          .key(newKey)
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.createMultipartUpload(correctRequest));
    }

    @Test
    public void testCompleteMultipartUpload() {
      String newKey = "complete-multipart-upload-key";
      CreateMultipartUploadResponse multipartUploadResponse =
          s3Client.createMultipartUpload(b -> b.bucket(defaultBucketName).key(newKey));

      String uploadId = multipartUploadResponse.uploadId();

      UploadPartResponse uploadPartResponse = s3Client.uploadPart(b -> b.bucket(defaultBucketName)
          .key(newKey)
          .uploadId(uploadId)
          .partNumber(1)
          .contentLength((long) testContent.getBytes(StandardCharsets.UTF_8).length)
          .build(), fromString(testContent));

      CompletedMultipartUpload completedUpload = CompletedMultipartUpload.builder()
          .parts(
              CompletedPart.builder().partNumber(1).eTag(uploadPartResponse.eTag()).build()
          ).build();



      CompleteMultipartUploadRequest wrongRequest = CompleteMultipartUploadRequest.builder()
          .bucket(defaultBucketName)
          .key(newKey)
          .uploadId(uploadId)
          .multipartUpload(completedUpload)
          .expectedBucketOwner(wrongOwner)
          .build();
      verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.completeMultipartUpload(wrongRequest));

      CompleteMultipartUploadRequest correctRequest = CompleteMultipartUploadRequest.builder()
          .bucket(defaultBucketName)
          .key(newKey)
          .uploadId(uploadId)
          .multipartUpload(completedUpload)
          .expectedBucketOwner(correctOwner)
          .build();
      verifyPassBucketOwnershipVerification(() -> s3Client.completeMultipartUpload(correctRequest));
    }
  }

  private void verifyPassBucketOwnershipVerification(Executable function) {
    assertDoesNotThrow(function);
  }

  private void verifyBucketOwnershipVerificationAccessDenied(Executable function) {
    S3Exception exception = assertThrows(S3Exception.class, function);
    assertEquals(403, exception.statusCode());
    assertEquals("AccessDenied", exception.awsErrorDetails().errorCode());
  }
}
