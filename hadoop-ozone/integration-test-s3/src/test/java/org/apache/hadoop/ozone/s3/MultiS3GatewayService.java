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

    // Parse host and port with proper error handling
    String[] parts = url.split(":");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid S3G HTTP address format: " + url);
    }

    String host = parts[0];
    int port;
    try {
      port = Integer.parseInt(parts[1]);
      if (port <= 0 || port > 65535) {
        throw new IllegalArgumentException("Invalid port number: " + port);
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid port format in address: " + url, e);
    }

    proxyServer = new ProxyServer(urls, host, port);
    proxyServer.start();
  }

  @Override
  public void stop() throws Exception {
    Exception lastException = null;

    // Stop proxy server first
    if (proxyServer != null) {
      try {
        proxyServer.stop();
      } catch (Exception e) {
        LOG.warn("Error stopping proxy server", e);
        lastException = e;
      }
    }

    // Stop all gateway services
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


  private void configureS3G(OzoneConfiguration conf) {
    OzoneConfigurationHolder.resetConfiguration();

    conf.set(S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY, localhostWithFreePort());
    conf.set(S3GatewayConfigKeys.OZONE_S3G_HTTPS_ADDRESS_KEY, localhostWithFreePort());

    OzoneConfigurationHolder.setConfiguration(conf);
  }
}
