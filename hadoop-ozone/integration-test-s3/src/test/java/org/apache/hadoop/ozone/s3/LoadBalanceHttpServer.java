package org.apache.hadoop.ozone.s3;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadBalanceHttpServer extends S3GatewayHttpServer {

  private static final Logger LOG =
      LoggerFactory.getLogger(LoadBalanceHttpServer.class);

  private List<String> urls;
  private final AtomicInteger counter = new AtomicInteger();

  private LoadBalanceHttpServer(MutableConfigurationSource conf,
                                String name) throws IOException {
    super(conf, name);
    this.addServlet("proxy", "/*", ReverseProxyServlet.class);
  }

  public LoadBalanceHttpServer(MutableConfigurationSource conf, String name, List<String> urls)
      throws IOException {
    this(conf, name);
    this.urls = urls;
    if (urls == null || urls.isEmpty()) {
      throw new IllegalArgumentException("URLs for reverse proxy cannot be null or empty");
    }
    LOG.info("Reverse proxy initialized with URLs: {}", urls);
  }

  public class ReverseProxyServlet extends HttpServlet {

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {


      String targetUrl = urls.get(counter.getAndIncrement() % urls.size());
      LOG.info("forwarding request to {}", targetUrl);

      // 3. 建立連線並複製請求
      HttpURLConnection conn = (HttpURLConnection) new URL(targetUrl).openConnection();
      conn.setRequestMethod(req.getMethod());
      conn.setDoInput(true);
      conn.setDoOutput(true);

      // 複製 header
      Enumeration<String> headerNames = req.getHeaderNames();
      while (headerNames.hasMoreElements()) {
        String header = headerNames.nextElement();
        if (!header.equalsIgnoreCase("Host")) {
          conn.setRequestProperty(header, req.getHeader(header));
        }
      }

      // 複製 body（如有）
      if (req.getContentLength() > 0) {
        conn.setDoOutput(true);
        try (ServletInputStream in = req.getInputStream();
             OutputStream out = conn.getOutputStream()) {
          copyStream(in, out);
        }
      }

      // 4. 取得回應並寫回 client
      int status = conn.getResponseCode();
      resp.setStatus(status);
      Map<String, List<String>> headerFields = conn.getHeaderFields();
      for (Map.Entry<String, List<String>> entry : headerFields.entrySet()) {
        String k = entry.getKey();
        List<String> v = entry.getValue();
        if (k != null && v != null) {
          for (String value : v) {
            resp.addHeader(k, value);
          }
        }
      }

      InputStream in = (status >= 400 ? conn.getErrorStream() : conn.getInputStream());
      ServletOutputStream out = resp.getOutputStream();
      if (in != null) {
        try {
          copyStream(in, out);
        } finally {
          in.close();
          out.close();
        }
      }
    }

    // Java 8 複製 stream 的工具方法
    private void copyStream(InputStream in, OutputStream out) throws IOException {
      byte[] buffer = new byte[8192];
      int len;
      while ((len = in.read(buffer)) != -1) {
        out.write(buffer, 0, len);
      }
    }

  }
}
