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

package org.apache.hadoop.ozone.container.common.volume;

import static org.apache.hadoop.ozone.container.common.volume.VolumeChoosingUtil.logIfSomeVolumesOutOfSpace;
import static org.apache.hadoop.ozone.container.common.volume.VolumeChoosingUtil.throwDiskOutOfSpace;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Volume choosing policy that randomly choose volume with remaining
 * space to satisfy the size constraints.
 * <p>
 * The Algorithm is as follows, Pick 2 random volumes from a given pool of
 * volumeSet and then pick the volume with lower utilization. This leads to a
 * higher probability of volume with lower utilization to be picked.
 * <p>
 * Same algorithm as the SCMContainerPlacementCapacity.
 */
public class CapacityVolumeChoosingPolicy implements VolumeChoosingPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(
      CapacityVolumeChoosingPolicy.class);

  @Override
  public synchronized HddsVolume chooseVolume(List<HddsVolume> volumes,
      long maxContainerSize) throws IOException {

    // No volumes available to choose from
    if (volumes.isEmpty()) {
      throw new DiskOutOfSpaceException("No more available volumes");
    }

    AvailableSpaceFilter filter = new AvailableSpaceFilter(maxContainerSize);

    List<HddsVolume> volumesWithEnoughSpace = volumes.stream()
        .filter(filter)
        .collect(Collectors.toList());

    if (volumesWithEnoughSpace.isEmpty()) {
      throwDiskOutOfSpace(filter, LOG);
    } else {
      logIfSomeVolumesOutOfSpace(filter, LOG);
    }

    int count = volumesWithEnoughSpace.size();
    HddsVolume selectedVolume = volumesWithEnoughSpace.get(0);
    if (count > 1) {
      // Even if we don't have too many volumes in volumesWithEnoughSpace, this
      // algorithm will still help us choose the volume with larger
      // available space than other volumes.
      // Say we have vol1 with more available space than vol2, for two choices,
      // the distribution of possibility is as follows:
      // 1. vol1 + vol2: 25%, result is vol1
      // 2. vol1 + vol1: 25%, result is vol1
      // 3. vol2 + vol1: 25%, result is vol1
      // 4. vol2 + vol2: 25%, result is vol2
      // So we have a total of 75% chances to choose vol1, which meets our
      // expectation.
      int firstIndex = ThreadLocalRandom.current().nextInt(count);
      int secondIndex = ThreadLocalRandom.current().nextInt(count);

      HddsVolume firstVolume = volumesWithEnoughSpace.get(firstIndex);
      HddsVolume secondVolume = volumesWithEnoughSpace.get(secondIndex);

      long firstAvailable = firstVolume.getCurrentUsage().getAvailable()
          - firstVolume.getCommittedBytes();
      long secondAvailable = secondVolume.getCurrentUsage().getAvailable()
          - secondVolume.getCommittedBytes();
      selectedVolume = firstAvailable < secondAvailable ? secondVolume : firstVolume;
    }
    selectedVolume.incCommittedBytes(maxContainerSize);
    return selectedVolume;
  }
}
