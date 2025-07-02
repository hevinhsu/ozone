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

package org.apache.hadoop.ozone.s3;

import static org.apache.ozone.test.GenericTestUtils.PortAllocator.localhostWithFreePort;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Multi S3 Gateway for {@link MiniOzoneCluster}. */
public class MultiS3GatewayService implements MiniOzoneCluster.Service {

  private static final Logger LOG = LoggerFactory.getLogger(MultiS3GatewayService.class);

  private final List<S3GatewayService> gatewayServices = new ArrayList<>();
  private ProxyServer proxyServer;
  private OzoneConfiguration conf;


  public MultiS3GatewayService(int numGateways) {
    for (int i = 0; i < numGateways; i++) {
      gatewayServices.add(new S3GatewayService());
    }
  }

  @Override
  public void start(OzoneConfiguration conf) throws Exception {
    List<String> urls = new ArrayList<>();
    for (S3GatewayService service : gatewayServices) {
      service.start(conf);
      String redirectUrl = "http://" + service.getConf().get(S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY);
      urls.add(redirectUrl);
    }

    this.conf = new OzoneConfiguration(conf);
    this.conf.set(S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY, localhostWithFreePort());
    String url = this.conf.get(S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY);
    String[] parts = url.split(":");
    proxyServer = new ProxyServer(urls, parts[0], Integer.parseInt(parts[1]));
    proxyServer.start();
  }

  @Override
  public void stop() throws Exception {
    Exception lastException = null;

    if (proxyServer != null) {
      try {
        proxyServer.stop();
      } catch (Exception e) {
        LOG.warn("Error stopping proxy server", e);
        lastException = e;
      }
    }

    for (S3GatewayService service : gatewayServices) {
      try {
        service.stop();
      } catch (Exception e) {
        LOG.warn("Error stopping S3 gateway service", e);
        lastException = e;
      }
    }

    if (lastException != null) {
      throw lastException;
    }
  }

  public OzoneConfiguration getConf() {
    return conf;
  }

}
