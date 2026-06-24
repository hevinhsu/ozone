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

package org.apache.hadoop.hdds.scm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Integration tests that verify rack/host topology is correctly propagated
 * to SCM and that pipeline placement respects rack boundaries.
 *
 * <p>Three scenarios are covered:
 * <ol>
 *   <li>{@link WithRacksAndHosts} - both racks and hostnames configured</li>
 *   <li>{@link WithRacksOnly} - racks configured, hostnames use cluster default</li>
 *   <li>{@link WithHostsOnly} - hostnames configured, no rack configuration
 *       (all nodes fall back to {@link NetworkTopology#DEFAULT_RACK})</li>
 * </ol>
 *
 * <p>See HDDS-14812.
 */
public class TestRackAwarePlacement {

  private static final String RACK0 = "/rack0";
  private static final String RACK1 = "/rack1";

  private static final String[] RACKS = {
      RACK0, RACK0, RACK0,
      RACK1, RACK1, RACK1
  };

  private static final String[] HOSTS = {
      "host0.test", "host1.test", "host2.test",
      "host3.test", "host4.test", "host5.test"
  };

  // -----------------------------------------------------------------------
  // Scenario 1: racks + hosts both configured
  // -----------------------------------------------------------------------

  /**
   * Verifies topology behaviour when both racks and hostnames are explicitly
   * configured. Pipelines should span multiple racks and each datanode should
   * report the configured hostname.
   */
  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class WithRacksAndHosts {

    private MiniOzoneCluster cluster;

    @BeforeAll
    void init() throws Exception {
      OzoneConfiguration conf = new OzoneConfiguration();
      cluster = MiniOzoneCluster.newBuilder(conf)
          .setRacks(RACKS)
          .setHosts(HOSTS)
          .build();
      cluster.waitForClusterToBeReady();
      cluster.waitForPipelineTobeReady(ReplicationFactor.THREE, 60_000);
    }

    @AfterAll
    void tearDown() {
      if (cluster != null) {
        cluster.shutdown();
      }
    }

    @Test
    void testDatanodesHaveCorrectRack() {
      assertRackAssignments(cluster, RACKS);
    }

    @Test
    void testDatanodesHaveCorrectHostname() {
      assertHostnameAssignments(cluster, HOSTS);
    }

    @Test
    void testRatisPipelineSpansMultipleRacks() {
      assertPipelinesSpanMultipleRacks(cluster);
    }
  }

  // -----------------------------------------------------------------------
  // Scenario 2: racks only, no explicit hosts
  // -----------------------------------------------------------------------

  /**
   * Verifies topology behaviour when only racks are configured and hostnames
   * are left to the cluster default. Pipelines should still span multiple
   * racks; hostname values are not asserted in this scenario.
   */
  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class WithRacksOnly {

    private MiniOzoneCluster cluster;

    @BeforeAll
    void init() throws Exception {
      OzoneConfiguration conf = new OzoneConfiguration();
      cluster = MiniOzoneCluster.newBuilder(conf)
          .setRacks(RACKS)
          .build();
      cluster.waitForClusterToBeReady();
      cluster.waitForPipelineTobeReady(ReplicationFactor.THREE, 60_000);
    }

    @AfterAll
    void tearDown() {
      if (cluster != null) {
        cluster.shutdown();
      }
    }

    @Test
    void testDatanodesHaveCorrectRack() {
      assertRackAssignments(cluster, RACKS);
    }

    @Test
    void testRatisPipelineSpansMultipleRacks() {
      assertPipelinesSpanMultipleRacks(cluster);
    }
  }

  // -----------------------------------------------------------------------
  // Scenario 3: hosts only, no racks
  // -----------------------------------------------------------------------

  /**
   * Verifies behaviour when only hostnames are configured and no rack
   * information is provided. All datanodes should fall back to
   * {@link NetworkTopology#DEFAULT_RACK} and each node should report the
   * configured hostname.
   *
   * <p>Pipelines are not expected to span multiple racks in this scenario.
   */
  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class WithHostsOnly {

    private MiniOzoneCluster cluster;

    @BeforeAll
    void init() throws Exception {
      OzoneConfiguration conf = new OzoneConfiguration();
      cluster = MiniOzoneCluster.newBuilder(conf)
          .setHosts(HOSTS)
          .build();
      cluster.waitForClusterToBeReady();
      cluster.waitForPipelineTobeReady(ReplicationFactor.THREE, 60_000);
    }

    @AfterAll
    void tearDown() {
      if (cluster != null) {
        cluster.shutdown();
      }
    }

    @Test
    void testDatanodesHaveCorrectHostname() {
      assertHostnameAssignments(cluster, HOSTS);
    }

    @Test
    void testDatanodesAllInDefaultRack() {
      NodeManager nodeManager =
          cluster.getStorageContainerManager().getScmNodeManager();
      List<? extends DatanodeDetails> allNodes = nodeManager.getAllNodes();

      for (DatanodeDetails dn : allNodes) {
        assertEquals(NetworkTopology.DEFAULT_RACK, dn.getNetworkLocation(),
            "Datanode " + dn.getHostName()
                + " should be in default rack when no racks are configured");
      }
    }
  }

  // -----------------------------------------------------------------------
  // Shared assertion helpers
  // -----------------------------------------------------------------------

  private void assertRackAssignments(MiniOzoneCluster cluster,
                                            String[] expectedRacks) {
    NodeManager nodeManager =
        cluster.getStorageContainerManager().getScmNodeManager();
    List<? extends DatanodeDetails> allNodes = nodeManager.getAllNodes();

    assertEquals(expectedRacks.length, allNodes.size(),
        "Number of registered datanodes should match number of configured racks");

    // Collect actual rack counts
    long actualRack0 = allNodes.stream()
        .filter(dn -> RACK0.equals(dn.getNetworkLocation()))
        .count();
    long actualRack1 = allNodes.stream()
        .filter(dn -> RACK1.equals(dn.getNetworkLocation()))
        .count();

    long expectedRack0 = Arrays.stream(expectedRacks).filter(RACK0::equals).count();
    long expectedRack1 = Arrays.stream(expectedRacks).filter(RACK1::equals).count();

    assertEquals(expectedRack0, actualRack0,
        "Expected " + expectedRack0 + " datanodes on " + RACK0);
    assertEquals(expectedRack1, actualRack1,
        "Expected " + expectedRack1 + " datanodes on " + RACK1);

    // Every node must be in a known rack
    for (DatanodeDetails dn : allNodes) {
      String location = dn.getNetworkLocation();
      assertNotNull(location,
          "Network location must not be null for " + dn.getHostName());
      assertTrue(location.equals(RACK0) || location.equals(RACK1),
          "Unexpected rack for datanode " + dn.getHostName() + ": " + location);
    }
  }

  private void assertHostnameAssignments(MiniOzoneCluster cluster,
                                                String[] expectedHosts) {
    NodeManager nodeManager =
        cluster.getStorageContainerManager().getScmNodeManager();
    List<? extends DatanodeDetails> allNodes = nodeManager.getAllNodes();

    assertEquals(expectedHosts.length, allNodes.size(),
        "Number of registered datanodes should match number of configured hosts");

    Set<String> actualHostnames = allNodes.stream()
        .map(DatanodeDetails::getHostName)
        .collect(Collectors.toSet());

    Set<String> expectedHostnames = Arrays.stream(expectedHosts)
        .collect(Collectors.toSet());

    assertEquals(expectedHostnames, actualHostnames,
        "Registered datanode hostnames should match configured hosts");
  }

  private void assertPipelinesSpanMultipleRacks(MiniOzoneCluster cluster) {
    List<Pipeline> pipelines = cluster.getStorageContainerManager()
        .getPipelineManager()
        .getPipelines(RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
            Pipeline.PipelineState.OPEN);

    assertFalse(pipelines.isEmpty(),
        "There should be at least one open RATIS THREE pipeline");

    for (Pipeline pipeline : pipelines) {
      Set<String> racks = pipeline.getNodes().stream()
          .map(DatanodeDetails::getNetworkLocation)
          .collect(Collectors.toSet());

      assertTrue(racks.size() >= 2,
          "Pipeline " + pipeline.getId()
              + " should span at least 2 racks, but spans: " + racks);
    }
  }
}
