package org.apache.hadoop.ozone.s3;

import static org.apache.ozone.test.GenericTestUtils.PortAllocator.localhostWithFreePort;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;

public class MultiS3GatewayService implements MiniOzoneCluster.Service {

  //  private LoadBalanceHttpServer reverseProxyHttpServer;
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

//    configureS3G(new OzoneConfiguration(conf));
//    reverseProxyHttpServer =
//        new LoadBalanceHttpServer(OzoneConfigurationHolder.configuration(), "s3GatewayReverseProxy", urls);
//    reverseProxyHttpServer.start();
//    configureS3G(new OzoneConfiguration(conf));
    this.conf = new OzoneConfiguration(conf);
    this.conf.set(S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY, localhostWithFreePort());
    String url = this.conf.get(S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY);
    String host = url.split(":")[0];
    int port = Integer.parseInt(url.split(":")[1]);
    proxyServer = new ProxyServer(urls, host, port);
    proxyServer.start();
  }

  @Override
  public void stop() throws Exception {
    for (S3GatewayService service : gatewayServices) {
      service.stop();
    }
//    reverseProxyHttpServer.stop();
    proxyServer.stop();
  }

  public OzoneConfiguration getConf() {
//    return OzoneConfigurationHolder.configuration();
    return conf;
  }


  private void configureS3G(OzoneConfiguration conf) {
    OzoneConfigurationHolder.resetConfiguration();

    conf.set(S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY, localhostWithFreePort());
    conf.set(S3GatewayConfigKeys.OZONE_S3G_HTTPS_ADDRESS_KEY, localhostWithFreePort());

    OzoneConfigurationHolder.setConfiguration(conf);
  }
}
