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

package org.apache.hadoop.hdds.upgrade;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationCheckpoint;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManagerImpl;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizationContext;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.upgrade.DefaultUpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.UpgradeTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests upgrade finalization failure scenarios and corner cases specific to SCM
 * HA, under different topology configurations (racks only, hosts only,
 * racks and hosts combined).
 */
public class TestScmHAFinalizationWithRacks {

  private static final String CLIENT_ID = UUID.randomUUID().toString();
  private static final Logger LOG =
      LoggerFactory.getLogger(TestScmHAFinalizationWithRacks.class);
  private static final String METHOD_SOURCE =
      "org.apache.hadoop.hdds.upgrade" +
          ".TestScmHAFinalizationWithRacks#injectionPointsToTest";

  private static final String[] RACKS = {"/rack0", "/rack1", "/rack0"};
  private static final String[] HOSTS = {"host0", "host1", "host2"};

  private StorageContainerLocationProtocol scmClient;
  private MiniOzoneHAClusterImpl cluster;
  private static final int NUM_DATANODES = 3;
  private static final int NUM_SCMS = 3;
  private Future<?> finalizationFuture;

  public void init(OzoneConfiguration conf,
                   UpgradeFinalizationExecutor<SCMUpgradeFinalizationContext> executor,
                   int numInactiveSCMs, String[] racks, String[] hosts) throws Exception {

    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setUpgradeFinalizationExecutor(executor);

    conf.setInt(SCMStorageConfig.TESTING_INIT_LAYOUT_VERSION_KEY,
        HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    conf.set(ScmConfigKeys.OZONE_SCM_HA_RATIS_SERVER_RPC_FIRST_ELECTION_TIMEOUT,
        "5s");

    MiniOzoneHAClusterImpl.Builder clusterBuilder =
        MiniOzoneCluster.newHABuilder(conf);
    clusterBuilder
        .setNumOfStorageContainerManagers(NUM_SCMS)
        .setNumOfActiveSCMs(NUM_SCMS - numInactiveSCMs)
        .setSCMServiceId("scmservice")
        .setNumOfOzoneManagers(1)
        .setSCMConfigurator(configurator)
        .setNumDatanodes(NUM_DATANODES)
        .setDatanodeFactory(UniformDatanodesFactory.newBuilder()
            .setLayoutVersion(
                HDDSLayoutFeature.INITIAL_VERSION.layoutVersion())
            .build());

    if (racks != null) {
      clusterBuilder.setRacks(racks);
    }
    if (hosts != null) {
      clusterBuilder.setHosts(hosts);
    }

    this.cluster = clusterBuilder.build();

    scmClient = cluster.getStorageContainerLocationClient();
    cluster.waitForClusterToBeReady();

    finalizationFuture = Executors.newSingleThreadExecutor().submit(
        () -> {
          try {
            scmClient.finalizeScmUpgrade(CLIENT_ID);
          } catch (IOException ex) {
            LOG.info("finalization client failed. This may be expected if " +
                "the test injected failures.", ex);
          }
        });
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Argument supplier for parameterized tests.
   */
  public static Stream<Arguments> injectionPointsToTest() {
    return Stream.of(
        Arguments.of(UpgradeTestInjectionPoints.AFTER_PRE_FINALIZE_UPGRADE),
        Arguments.of(UpgradeTestInjectionPoints.AFTER_COMPLETE_FINALIZATION),
        Arguments.of(UpgradeTestInjectionPoints.AFTER_POST_FINALIZE_UPGRADE)
    );
  }

  // -------------------------------------------------------------------------
  // testFinalizationWithLeaderChange
  // -------------------------------------------------------------------------

  @ParameterizedTest
  @MethodSource(METHOD_SOURCE)
  public void testFinalizationWithLeaderChangeRacksAndHosts(
      UpgradeTestInjectionPoints haltingPoint) throws Exception {
    runFinalizationWithLeaderChange(haltingPoint, RACKS, HOSTS);
  }

  @ParameterizedTest
  @MethodSource(METHOD_SOURCE)
  public void testFinalizationWithLeaderChangeRacksOnly(
      UpgradeTestInjectionPoints haltingPoint) throws Exception {
    runFinalizationWithLeaderChange(haltingPoint, RACKS, null);
  }

  @ParameterizedTest
  @MethodSource(METHOD_SOURCE)
  public void testFinalizationWithLeaderChangeHostsOnly(
      UpgradeTestInjectionPoints haltingPoint) throws Exception {
    runFinalizationWithLeaderChange(haltingPoint, null, HOSTS);
  }

  private void runFinalizationWithLeaderChange(
      UpgradeTestInjectionPoints haltingPoint,
      String[] racks, String[] hosts) throws Exception {

    CountDownLatch pauseLatch = new CountDownLatch(1);
    CountDownLatch unpauseLatch = new CountDownLatch(1);
    init(new OzoneConfiguration(),
        UpgradeTestUtils.newPausingFinalizationExecutor(haltingPoint,
            pauseLatch, unpauseLatch, LOG),
        0, racks, hosts);
    pauseLatch.await();

    StorageContainerManager oldLeaderScm = cluster.getActiveSCM();
    LOG.info("Stopping current SCM leader {} to initiate a leader change.",
        oldLeaderScm.getSCMNodeId());
    cluster.shutdownStorageContainerManager(oldLeaderScm);

    cluster.waitForClusterToBeReady();

    checkMidFinalizationConditions(haltingPoint,
        cluster.getStorageContainerManagersList());

    cluster.restartStorageContainerManager(oldLeaderScm, true);

    StorageContainerManager newLeaderScm = cluster.getActiveSCM();
    assertNotEquals(newLeaderScm.getSCMNodeId(),
        oldLeaderScm.getSCMNodeId());

    unpauseLatch.countDown();

    finalizationFuture.get();
    TestHddsUpgradeUtils.waitForFinalizationFromClient(scmClient, CLIENT_ID);
    waitForScmsToFinalize(cluster.getStorageContainerManagersList());

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        cluster.getStorageContainerManagersList(), 0, NUM_DATANODES);
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes(), 0, CLOSED);
  }

  // -------------------------------------------------------------------------
  // testFinalizationWithRestart
  // -------------------------------------------------------------------------

  @ParameterizedTest
  @MethodSource(METHOD_SOURCE)
  @Flaky("HDDS-8714")
  public void testFinalizationWithRestartRacksAndHosts(
      UpgradeTestInjectionPoints haltingPoint) throws Exception {
    runFinalizationWithRestart(haltingPoint, RACKS, HOSTS);
  }

  @ParameterizedTest
  @MethodSource(METHOD_SOURCE)
  @Flaky("HDDS-8714")
  public void testFinalizationWithRestartRacksOnly(
      UpgradeTestInjectionPoints haltingPoint) throws Exception {
    runFinalizationWithRestart(haltingPoint, RACKS, null);
  }

  @ParameterizedTest
  @MethodSource(METHOD_SOURCE)
  @Flaky("HDDS-8714")
  public void testFinalizationWithRestartHostsOnly(
      UpgradeTestInjectionPoints haltingPoint) throws Exception {
    runFinalizationWithRestart(haltingPoint, null, HOSTS);
  }

  private void runFinalizationWithRestart(
      UpgradeTestInjectionPoints haltingPoint,
      String[] racks, String[] hosts) throws Exception {

    CountDownLatch terminateLatch = new CountDownLatch(1);
    init(new OzoneConfiguration(),
        UpgradeTestUtils.newTerminatingFinalizationExecutor(haltingPoint,
            terminateLatch, LOG),
        0, racks, hosts);
    terminateLatch.await();

    LOG.info("Restarting all SCMs during upgrade finalization.");
    cluster.getSCMConfigurator()
        .setUpgradeFinalizationExecutor(
            new DefaultUpgradeFinalizationExecutor<>());
    List<StorageContainerManager> originalSCMs =
        cluster.getStorageContainerManagers();

    for (StorageContainerManager scm : originalSCMs) {
      cluster.restartStorageContainerManager(scm, false);
    }

    checkMidFinalizationConditions(haltingPoint,
        cluster.getStorageContainerManagersList());

    cluster.waitForClusterToBeReady();

    finalizationFuture.get();
    TestHddsUpgradeUtils.waitForFinalizationFromClient(scmClient, CLIENT_ID);
    waitForScmsToFinalize(cluster.getStorageContainerManagersList());

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        cluster.getStorageContainerManagersList(), 0, NUM_DATANODES);
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes(), 0, CLOSED);
  }

  // -------------------------------------------------------------------------
  // testSnapshotFinalization
  // -------------------------------------------------------------------------

  @Test
  public void testSnapshotFinalizationRacksAndHosts() throws Exception {
    runSnapshotFinalization(RACKS, HOSTS);
  }

  @Test
  public void testSnapshotFinalizationRacksOnly() throws Exception {
    runSnapshotFinalization(RACKS, null);
  }

  @Test
  public void testSnapshotFinalizationHostsOnly() throws Exception {
    runSnapshotFinalization(null, HOSTS);
  }

  private void runSnapshotFinalization(String[] racks, String[] hosts)
      throws Exception {

    int numInactiveSCMs = 1;
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_PURGE_ENABLED, true);
    conf.setInt(ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_PURGE_GAP, 5);
    conf.setLong(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD, 5);

    init(conf, new DefaultUpgradeFinalizationExecutor<>(),
        numInactiveSCMs, racks, hosts);

    LogCapturer logCapture =
        LogCapturer.captureLogs(FinalizationStateManagerImpl.class);

    StorageContainerManager inactiveScm = cluster.getInactiveSCM().next();
    LOG.info("Inactive SCM node ID: {}", inactiveScm.getSCMNodeId());

    List<StorageContainerManager> scms =
        cluster.getStorageContainerManagersList();
    List<StorageContainerManager> activeScms = new ArrayList<>();
    for (StorageContainerManager scm : scms) {
      if (!scm.getSCMNodeId().equals(inactiveScm.getSCMNodeId())) {
        activeScms.add(scm);
      }
    }

    finalizationFuture.get();
    TestHddsUpgradeUtils.waitForFinalizationFromClient(scmClient, CLIENT_ID);
    waitForScmsToFinalize(activeScms);

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        activeScms, 0, NUM_DATANODES);
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes(), 0, CLOSED);

    for (int i = 0; i < 10; i++) {
      ContainerWithPipeline container =
          scmClient.allocateContainer(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE, "owner");
      scmClient.closeContainer(
          container.getContainerInfo().getContainerID());
    }

    cluster.startInactiveSCM(inactiveScm.getSCMNodeId());
    waitForScmToFinalize(inactiveScm);

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        inactiveScm, 0, NUM_DATANODES);

    assertThat(logCapture.getOutput()).contains("New SCM snapshot " +
        "received with metadata layout version");
  }

  // -------------------------------------------------------------------------
  // Shared helpers
  // -------------------------------------------------------------------------

  private void waitForScmsToFinalize(Collection<StorageContainerManager> scms)
      throws Exception {
    for (StorageContainerManager scm : scms) {
      waitForScmToFinalize(scm);
    }
  }

  private void waitForScmToFinalize(StorageContainerManager scm)
      throws Exception {
    GenericTestUtils.waitFor(() -> !scm.isInSafeMode(), 500, 5000);
    GenericTestUtils.waitFor(() -> {
      FinalizationCheckpoint checkpoint =
          scm.getScmContext().getFinalizationCheckpoint();
      LOG.info("Waiting for SCM {} (leader? {}) to finalize. Current " +
              "finalization checkpoint is {}",
          scm.getSCMNodeId(), scm.checkLeader(), checkpoint);
      return checkpoint.hasCrossed(
          FinalizationCheckpoint.FINALIZATION_COMPLETE);
    }, 2_000, 60_000);
  }

  private void checkMidFinalizationConditions(
      UpgradeTestInjectionPoints haltingPoint,
      List<StorageContainerManager> scms) {

    switch (haltingPoint) {
    case BEFORE_PRE_FINALIZE_UPGRADE:
      assertTrue(scms.stream().anyMatch(scm ->
          scm.getScmContext().getFinalizationCheckpoint()
              == FinalizationCheckpoint.FINALIZATION_REQUIRED));
      assertTrue(scms.stream().noneMatch(scm ->
          scm.getPipelineManager().isPipelineCreationFrozen()));
      break;
    case AFTER_PRE_FINALIZE_UPGRADE:
      assertTrue(scms.stream().anyMatch(scm ->
          scm.getScmContext().getFinalizationCheckpoint()
              == FinalizationCheckpoint.FINALIZATION_STARTED));
      assertTrue(scms.stream()
          .filter(scm ->
              scm.getScmContext().getFinalizationCheckpoint()
                  == FinalizationCheckpoint.FINALIZATION_STARTED)
          .allMatch(scm ->
              scm.getPipelineManager().isPipelineCreationFrozen()));
      break;
    case AFTER_COMPLETE_FINALIZATION:
      assertTrue(scms.stream().anyMatch(scm ->
          scm.getScmContext().getFinalizationCheckpoint()
              == FinalizationCheckpoint.MLV_EQUALS_SLV));
      assertTrue(scms.stream()
          .filter(scm ->
              scm.getScmContext().getFinalizationCheckpoint()
                  == FinalizationCheckpoint.MLV_EQUALS_SLV)
          .noneMatch(scm ->
              scm.getPipelineManager().isPipelineCreationFrozen()));
      break;
    case AFTER_POST_FINALIZE_UPGRADE:
      assertTrue(scms.stream().anyMatch(scm ->
          scm.getScmContext().getFinalizationCheckpoint()
              == FinalizationCheckpoint.FINALIZATION_COMPLETE));
      assertTrue(scms.stream()
          .filter(scm ->
              scm.getScmContext().getFinalizationCheckpoint()
                  == FinalizationCheckpoint.FINALIZATION_COMPLETE)
          .noneMatch(scm ->
              scm.getPipelineManager().isPipelineCreationFrozen()));
      break;
    default:
      fail("Unknown halting point in test: " + haltingPoint);
    }
  }
}
