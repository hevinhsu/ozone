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

package org.apache.hadoop.ozone.om.helpers;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.security.GDPRSymmetricKey;

/**
 * Args for key. Client use this to specify key's attributes on  key creation
 * (putKey()).
 */
public final class OmKeyArgs implements Auditable {
  private final String volumeName;
  private final String bucketName;
  private final String keyName;
  private final String ownerName;
  private long dataSize;
  private final ReplicationConfig replicationConfig;
  private List<OmKeyLocationInfo> locationInfoList;
  private final boolean isMultipartKey;
  private final String multipartUploadID;
  private final int multipartUploadPartNumber;
  private final Map<String, String> metadata;
  private final boolean sortDatanodesInPipeline;
  private final List<OzoneAcl> acls;
  private final boolean latestVersionLocation;
  private final boolean recursive;
  private final boolean headOp;
  private final boolean forceUpdateContainerCacheFromSCM;
  private final Map<String, String> tags;
  // expectedDataGeneration, when used in key creation indicates that a
  // key with the same keyName should exist with the given generation.
  // For a key commit to succeed, the original key should still be present with the
  // generation unchanged.
  // This allows a key to be created an committed atomically if the original has not
  // been modified.
  private Long expectedDataGeneration = null;

  private OmKeyArgs(Builder b) {
    this.volumeName = b.volumeName;
    this.bucketName = b.bucketName;
    this.keyName = b.keyName;
    this.dataSize = b.dataSize;
    this.replicationConfig = b.replicationConfig;
    this.locationInfoList = b.locationInfoList;
    this.isMultipartKey = b.isMultipartKey;
    this.multipartUploadID = b.multipartUploadID;
    this.multipartUploadPartNumber = b.multipartUploadPartNumber;
    this.metadata = b.metadata;
    this.acls = b.acls;
    this.sortDatanodesInPipeline = b.sortDatanodesInPipeline;
    this.latestVersionLocation = b.latestVersionLocation;
    this.recursive = b.recursive;
    this.headOp = b.headOp;
    this.forceUpdateContainerCacheFromSCM = b.forceUpdateContainerCacheFromSCM;
    this.ownerName = b.ownerName;
    this.tags = b.tags;
    this.expectedDataGeneration = b.expectedDataGeneration;
  }

  public boolean getIsMultipartKey() {
    return isMultipartKey;
  }

  public String getMultipartUploadID() {
    return multipartUploadID;
  }

  public int getMultipartUploadPartNumber() {
    return multipartUploadPartNumber;
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  public List<OzoneAcl> getAcls() {
    return acls;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getKeyName() {
    return keyName;
  }

  public String getOwner() {
    return ownerName;
  }

  public long getDataSize() {
    return dataSize;
  }

  public void setDataSize(long size) {
    dataSize = size;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setLocationInfoList(List<OmKeyLocationInfo> locationInfoList) {
    this.locationInfoList = locationInfoList;
  }

  public List<OmKeyLocationInfo> getLocationInfoList() {
    return locationInfoList;
  }

  public boolean getSortDatanodes() {
    return sortDatanodesInPipeline;
  }

  public boolean getLatestVersionLocation() {
    return latestVersionLocation;
  }

  public boolean isRecursive() {
    return recursive;
  }

  public boolean isHeadOp() {
    return headOp;
  }

  public boolean isForceUpdateContainerCacheFromSCM() {
    return forceUpdateContainerCacheFromSCM;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public Long getExpectedDataGeneration() {
    return expectedDataGeneration;
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, this.volumeName);
    auditMap.put(OzoneConsts.BUCKET, this.bucketName);
    auditMap.put(OzoneConsts.KEY, this.keyName);
    auditMap.put(OzoneConsts.OWNER, this.ownerName);
    auditMap.put(OzoneConsts.DATA_SIZE, String.valueOf(this.dataSize));
    auditMap.put(OzoneConsts.REPLICATION_CONFIG,
        (this.replicationConfig != null) ?
            this.replicationConfig.toString() : null);
    return auditMap;
  }

  @VisibleForTesting
  public void addLocationInfo(OmKeyLocationInfo locationInfo) {
    if (this.locationInfoList == null) {
      locationInfoList = new ArrayList<>();
    }
    locationInfoList.add(locationInfo);
  }

  public OmKeyArgs.Builder toBuilder() {
    OmKeyArgs.Builder builder = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setOwnerName(ownerName)
        .setDataSize(dataSize)
        .setReplicationConfig(replicationConfig)
        .setLocationInfoList(locationInfoList)
        .setIsMultipartKey(isMultipartKey)
        .setMultipartUploadID(multipartUploadID)
        .setMultipartUploadPartNumber(multipartUploadPartNumber)
        .addAllMetadata(metadata)
        .setSortDatanodesInPipeline(sortDatanodesInPipeline)
        .setHeadOp(headOp)
        .setLatestVersionLocation(latestVersionLocation)
        .setAcls(acls)
        .setForceUpdateContainerCacheFromSCM(forceUpdateContainerCacheFromSCM)
        .addAllTags(tags);

    if (expectedDataGeneration != null) {
      builder.setExpectedDataGeneration(expectedDataGeneration);
    }

    return builder;
  }

  @Nonnull
  public KeyArgs toProtobuf() {
    KeyArgs.Builder builder = KeyArgs.newBuilder()
        .setVolumeName(getVolumeName())
        .setBucketName(getBucketName())
        .setKeyName(getKeyName())
        .setDataSize(getDataSize())
        .setSortDatanodes(getSortDatanodes())
        .setLatestVersionLocation(getLatestVersionLocation())
        .setHeadOp(isHeadOp())
        .setForceUpdateContainerCacheFromSCM(
            isForceUpdateContainerCacheFromSCM()
        );
    if (multipartUploadPartNumber != 0) {
      builder.setMultipartNumber(multipartUploadPartNumber);
    }
    if (expectedDataGeneration != null) {
      builder.setExpectedDataGeneration(expectedDataGeneration);
    }
    return builder.build();
  }

  /**
   * Builder class of OmKeyArgs.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private String keyName;
    private String ownerName;
    private long dataSize;
    private ReplicationConfig replicationConfig;
    private List<OmKeyLocationInfo> locationInfoList;
    private boolean isMultipartKey;
    private String multipartUploadID;
    private int multipartUploadPartNumber;
    private final Map<String, String> metadata = new HashMap<>();
    private boolean sortDatanodesInPipeline;
    private boolean latestVersionLocation;
    private List<OzoneAcl> acls;
    private boolean recursive;
    private boolean headOp;
    private boolean forceUpdateContainerCacheFromSCM;
    private final Map<String, String> tags = new HashMap<>();
    private Long expectedDataGeneration = null;

    public Builder setVolumeName(String volume) {
      this.volumeName = volume;
      return this;
    }

    public Builder setBucketName(String bucket) {
      this.bucketName = bucket;
      return this;
    }

    public Builder setKeyName(String key) {
      this.keyName = key;
      return this;
    }

    public Builder setOwnerName(String owner) {
      this.ownerName = owner;
      return this;
    }

    public Builder setDataSize(long size) {
      this.dataSize = size;
      return this;
    }

    public Builder setReplicationConfig(ReplicationConfig replConfig) {
      this.replicationConfig = replConfig;
      return this;
    }

    public Builder setLocationInfoList(List<OmKeyLocationInfo> locationInfos) {
      this.locationInfoList = locationInfos;
      return this;
    }

    public Builder setAcls(List<OzoneAcl> listOfAcls) {
      this.acls = listOfAcls;
      return this;
    }

    public Builder setIsMultipartKey(boolean isMultipart) {
      this.isMultipartKey = isMultipart;
      return this;
    }

    public Builder setMultipartUploadID(String uploadID) {
      this.multipartUploadID = uploadID;
      return this;
    }

    public Builder setMultipartUploadPartNumber(int multipartUploadPartNumber) {
      this.multipartUploadPartNumber = multipartUploadPartNumber;
      return this;
    }

    public Builder addMetadata(String key, String value) {
      this.metadata.put(key, value);
      return this;
    }

    public Builder addAllMetadata(Map<String, String> metadatamap) {
      this.metadata.putAll(metadatamap);
      return this;
    }

    public Builder addAllMetadataGdpr(Map<String, String> metadatamap) {
      addAllMetadata(metadatamap);
      if (Boolean.parseBoolean(metadata.get(OzoneConsts.GDPR_FLAG))) {
        GDPRSymmetricKey.newDefaultInstance().acceptKeyDetails(metadata::put);
      }
      return this;
    }

    public Builder addTag(String key, String value) {
      this.tags.put(key, value);
      return this;
    }

    public Builder addAllTags(Map<String, String> tagmap) {
      this.tags.putAll(tagmap);
      return this;
    }

    public Builder setSortDatanodesInPipeline(boolean sort) {
      this.sortDatanodesInPipeline = sort;
      return this;
    }

    public Builder setLatestVersionLocation(boolean latest) {
      this.latestVersionLocation = latest;
      return this;
    }

    public Builder setRecursive(boolean isRecursive) {
      this.recursive = isRecursive;
      return this;
    }

    public Builder setHeadOp(boolean isHeadOp) {
      this.headOp = isHeadOp;
      return this;
    }

    public Builder setForceUpdateContainerCacheFromSCM(boolean value) {
      this.forceUpdateContainerCacheFromSCM = value;
      return this;
    }

    public Builder setExpectedDataGeneration(long generation) {
      this.expectedDataGeneration = generation;
      return this;
    }

    public OmKeyArgs build() {
      return new OmKeyArgs(this);
    }

  }
}
