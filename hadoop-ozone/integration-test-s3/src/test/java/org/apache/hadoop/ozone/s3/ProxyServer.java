package org.apache.hadoop.ozone.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyServer {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyServer.class);
  private final List<String> s3gEndpoints;
  private final AtomicInteger counter = new AtomicInteger(0);
  private final HttpServer server;
  private final String host;
  private final int port;

  public ProxyServer(List<String> s3gEndpoints, String host, int proxyPort) throws Exception {
    this.s3gEndpoints = s3gEndpoints;
    this.host = host;
    this.port = proxyPort;
    server = HttpServer.create(new InetSocketAddress(host, proxyPort), 0);
    server.createContext("/", new ProxyHandler());
  }

  public void start() {
    server.start();
    LOG.info("Proxy started on http://{}:{}", host, port);

  }

  public void stop() {
    server.stop(0);
    LOG.info("Proxy stopped on http://{}:{}", host, port);
  }

  private class ProxyHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      try {
        String target = getNextEndpoint() + exchange.getRequestURI().toString();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        LOG.info("Received request and forwarding request to {}", target);
        HttpURLConnection conn = (HttpURLConnection) new URL(target).openConnection();
        String requestMethod = exchange.getRequestMethod();
        conn.setRequestMethod(requestMethod);

        copyHeaders(exchange, conn);

        // 複製 request body 並設置 Content-Length
        copyRequestBody(exchange, conn);

        int responseCode = conn.getResponseCode();

        // copy response headers
        for (String headerName : conn.getHeaderFields().keySet()) {
          if (headerName != null && !isHopByHopHeader(headerName)) {
            List<String> headerValues = conn.getHeaderFields().get(headerName);
            for (String headerValue : headerValues) {
              exchange.getResponseHeaders().add(headerName, headerValue);
            }
          }
        }

        exchange.sendResponseHeaders(responseCode, conn.getContentLengthLong());

        try (InputStream is = responseCode >= 400 ? conn.getErrorStream() : conn.getInputStream();
             OutputStream os = exchange.getResponseBody()) {
          if (is != null) {
            transferTo(is, os);
          }
        }

        conn.disconnect();
        if (responseCode >= 400) {
          LOG.error("Forwarded request to [{}] {} with response code {}", requestMethod, target, responseCode);
        } else {
          LOG.info("Forwarded request to [{}] {} with response code {}", requestMethod, target, responseCode);
        }
      } catch (Exception e) {
        LOG.error("Error forwarding request to S3G endpoint", e);
        throw new IOException("Failed to forward request", e);
      } finally {
        exchange.close();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
      }
    }

    private void copyRequestBody(HttpExchange exchange, HttpURLConnection conn) throws IOException {
      byte[] bodyBytes = IOUtils.toByteArray(exchange.getRequestBody());
      conn.setRequestProperty("Content-Length", String.valueOf(bodyBytes.length));
      LOG.info("write Content-Length to header: {}" , bodyBytes.length);
      if (bodyBytes.length > 0) {
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
          os.write(bodyBytes);
        }
      }
    }

    private void copyHeaders(HttpExchange exchange, HttpURLConnection conn) {
      for (String headerName : exchange.getRequestHeaders().keySet()) {
        if (!isHopByHopHeader(headerName)) {
          List<String> headerValues = exchange.getRequestHeaders().get(headerName);
          System.out.println();
          System.out.println();
          LOG.info("try to copy Header {} = {} to target header", headerName, headerValues);
          if (headerValues != null && !headerValues.isEmpty()) {
            String joined = String.join(",", headerValues);
            conn.setRequestProperty(headerName, joined);
            LOG.info("check [{}] after copy Header. expected: {}, actual: {}", headerName, headerValues, conn.getRequestProperty(headerName));
          }
        }
      }


      if (exchange.getRequestHeaders().size() != conn.getRequestProperties().size()) {
        LOG.info("\u001B[33m before header size: {}, after header size: {} \u001B[0m",
            exchange.getRequestHeaders().size(), conn.getRequestProperties().size());

        StringBuilder diff = new StringBuilder();
        diff.append("Request headers not matching after forwarding. Diff:\n");
        for (String header : exchange.getRequestHeaders().keySet()) {
          if (!conn.getRequestProperties().containsKey(header) ) {
            if (isHopByHopHeader(header)) {
              diff.append("[Hop-by-hop header] ");
            }
            diff.append("Missing in conn: ").append(header).append(" values: ")
                .append(exchange.getRequestHeaders().get(header)).append("\n");
          }
        }
        for (String header : conn.getRequestProperties().keySet()) {
          if (!exchange.getRequestHeaders().containsKey(header)) {
            diff.append("Added in conn: ").append(header).append(" values: ")
                .append(conn.getRequestProperties().get(header)).append("\n");
          }
        }
        LOG.info(diff.toString());
      }
    }
  }

  private String getNextEndpoint() {
    int idx = counter.getAndUpdate(i -> (i + 1) % s3gEndpoints.size());
    return s3gEndpoints.get(idx);
  }

  public void transferTo(InputStream in, OutputStream out) throws IOException {
    byte[] buffer = new byte[8192];
    int read;
    while ((read = in.read(buffer)) != -1) {
      out.write(buffer, 0, read);
    }
  }

  private boolean isHopByHopHeader(String header) {
    String lowerName = header.toLowerCase();
    return "connection".equals(lowerName) || "keep-alive".equals(lowerName)
        || "proxy-authenticate".equals(lowerName) || "proxy-authorization".equals(lowerName)
        || "te".equals(lowerName) || "trailers".equals(lowerName)
        || "transfer-encoding".equals(lowerName) || "upgrade".equals(lowerName);
  }
}
