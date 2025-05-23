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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Part;

/**
 * This class represents multipart list, which is required for
 * CompleteMultipart upload request.
 */
public class OmMultipartUploadCompleteList {

  private final LinkedHashMap<Integer, String> multipartMap;

  /**
   * Construct OmMultipartUploadCompleteList which holds multipart map which
   * contains part number and part name.
   * @param partMap
   */
  public OmMultipartUploadCompleteList(Map<Integer, String> partMap) {
    this.multipartMap = new LinkedHashMap<>(partMap);
  }

  /**
   * Return multipartMap which is a map of part number and part name.
   * @return multipartMap
   */
  public Map<Integer, String> getMultipartMap() {
    return multipartMap;
  }

  /**
   * Construct Part list from the multipartMap.
   * @return List<Part>
   */
  public List<Part> getPartsList() {
    List<Part> partList = new ArrayList<>();
    multipartMap.forEach((partNumber, eTag) -> partList.add(Part
        // set partName equal to eTag for back compatibility (partName is a required property)
        .newBuilder().setPartName(eTag).setETag(eTag).setPartNumber(partNumber).build()));
    return partList;
  }
}
