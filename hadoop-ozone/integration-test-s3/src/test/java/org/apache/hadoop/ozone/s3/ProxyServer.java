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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.collections.CollectionUtils;
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
  private final ExecutorService executor;

  // Add timeout configurations for better handling of large file operations
  private static final int CONNECT_TIMEOUT_MS = 60000; // 60 seconds
  private static final int READ_TIMEOUT_MS = 300000;   // 5 minutes for large file operations

  public ProxyServer(List<String> s3gEndpoints, String host, int proxyPort) throws Exception {
    this.s3gEndpoints = s3gEndpoints;
    this.host = host;
    this.port = proxyPort;
    server = HttpServer.create(new InetSocketAddress(host, proxyPort), 0);
    server.createContext("/", new ProxyHandler());

    this.executor = Executors.newCachedThreadPool();
    server.setExecutor(executor);

    LOG.info("ProxyServer initialized with endpoints: {}", s3gEndpoints);
    LOG.info("Timeout settings - Connect: {}ms, Read: {}ms", CONNECT_TIMEOUT_MS, READ_TIMEOUT_MS);
  }

  public void start() {
    server.start();
    LOG.info("Proxy started on http://{}:{}", host, port);

  }

  public void stop() {
    server.stop(0);
    executor.shutdownNow();
    LOG.info("Proxy stopped on http://{}:{}", host, port);
  }

  private class ProxyHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      HttpURLConnection conn = null;
      try {
        String target = getNextEndpoint() + exchange.getRequestURI().toString();
        conn = createConnection(exchange, target);
        String requestMethod = conn.getRequestMethod();

        LOG.info("Received request and forwarding request to [{}] {}", requestMethod, target);

        copyRequestHeaders(exchange, conn);
        addForwardedHeaders(exchange, conn);
        copyRequestBody(exchange, conn);

        int responseCode = conn.getResponseCode();

        copyResponseHeaders(exchange, conn);

        if (requestMethod.equals("HEAD") || responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
          exchange.sendResponseHeaders(responseCode, -1);
          return;
        }

        copyResponseBody(exchange, conn, responseCode);

      } catch (Exception e) {
        LOG.error("Error forwarding request to S3G endpoint", e);
        exchange.sendResponseHeaders(502, 0);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(("Proxy error: " + e.getMessage()).getBytes());
        }
      } finally {
        if (conn != null) {
          conn.disconnect();
        }
        exchange.close();
      }
    }

    private void addForwardedHeaders(HttpExchange exchange, HttpURLConnection conn) {
      String remoteAddr = exchange.getRemoteAddress().getAddress().getHostAddress();
      String xff = exchange.getRequestHeaders().getFirst("X-Forwarded-For");
      if (xff != null && !xff.isEmpty()) {
        conn.setRequestProperty("X-Forwarded-For", xff + ", " + remoteAddr);
      } else {
        conn.setRequestProperty("X-Forwarded-For", remoteAddr);
      }

      conn.setRequestProperty("X-Forwarded-Proto", "http");
    }

    private void copyResponseBody(HttpExchange exchange, HttpURLConnection conn, int responseCode) throws IOException {
      String transferEncoding = conn.getHeaderField("Transfer-Encoding");
      String contentLengthStr = conn.getHeaderField("Content-Length");
      long contentLength = -1;
      if (transferEncoding != null && transferEncoding.equalsIgnoreCase("chunked")) {
        exchange.sendResponseHeaders(responseCode, 0);
      } else if (contentLengthStr != null) {
        try {
          contentLength = Long.parseLong(contentLengthStr);
        } catch (NumberFormatException ignore) {
        }
        exchange.sendResponseHeaders(responseCode, contentLength);
      } else {
        exchange.sendResponseHeaders(responseCode, 0);
      }
      long copied = 0;
      try (InputStream is = responseCode >= 400 ? conn.getErrorStream() : conn.getInputStream();
           OutputStream os = exchange.getResponseBody()) {
        if (is != null) {
          copied = IOUtils.copyLarge(is, os, new byte[64 * 1024]);
          os.flush();
        }
      }
      LOG.info("Proxy copied response body, code={}, bytes={}", responseCode, copied);
    }

    private HttpURLConnection createConnection(HttpExchange exchange, String target) throws IOException {
      HttpURLConnection conn = (HttpURLConnection) new URL(target).openConnection();
      String requestMethod = exchange.getRequestMethod();
      conn.setRequestMethod(requestMethod);
      // Use extended timeouts for multipart upload operations and large file transfers
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      return conn;
    }

    private void copyRequestBody(HttpExchange exchange, HttpURLConnection conn) throws IOException {
      String method = exchange.getRequestMethod().toLowerCase();
      if (method.equals("post") || method.equals("put") || method.equals("patch") || method.equals("delete")) {
        List<String> contentLengths = exchange.getRequestHeaders().get("Content-Length");
        String transferEncoding = exchange.getRequestHeaders().getFirst("Transfer-Encoding");
        conn.setDoOutput(true);
        if (contentLengths != null && !contentLengths.isEmpty()) {
          try {
            int len = Integer.parseInt(contentLengths.get(0));
            conn.setFixedLengthStreamingMode(len);
            LOG.info("Proxy set fixed length streaming mode: {} bytes", len);
          } catch (NumberFormatException e) {
            LOG.warn("Invalid Content-Length: {}", contentLengths.get(0));
            conn.setChunkedStreamingMode(0);
          }
        } else if ("chunked".equalsIgnoreCase(transferEncoding)) {
          conn.setChunkedStreamingMode(0);
          LOG.info("Proxy set chunked streaming mode");
        } else {
          conn.setChunkedStreamingMode(0);
          LOG.info("Proxy set chunked streaming mode (default)");
        }
        InputStream reqBody = exchange.getRequestBody();
        try (OutputStream os = conn.getOutputStream()) {
          int copied = IOUtils.copy(reqBody, os);
          LOG.info("Proxy copied request body, method={}, bytes={}", method, copied);
        }
      }
    }

    private void copyRequestHeaders(HttpExchange exchange, HttpURLConnection conn) {
      for (String headerName : exchange.getRequestHeaders().keySet()) {
        if (isHopByHopHeader(headerName)) {
          continue;
        }

        // Skip Expect header to avoid 100-continue issue
        if ("Expect".equalsIgnoreCase(headerName)) {
          continue;
        }

        List<String> headerValues = exchange.getRequestHeaders().get(headerName);
        if (CollectionUtils.isEmpty(headerValues)) {
          continue;
        }

        for (String value : headerValues) {
          // Convert header names to lowercase because HttpServer capitalizes the first letter,
          // but s3g expects lowercase header names.
          conn.addRequestProperty(headerName.toLowerCase(), value);
          conn.addRequestProperty(headerName.toLowerCase(), value);
        }
      }
    }
  }

  private void copyResponseHeaders(HttpExchange exchange, HttpURLConnection conn) {
    for (String headerName : conn.getHeaderFields().keySet()) {
      if (headerName != null && !isHopByHopHeader(headerName)) {
        List<String> headerValues = conn.getHeaderFields().get(headerName);
        if (headerValues != null) {
          for (String headerValue : headerValues) {
            exchange.getResponseHeaders().add(headerName, headerValue);
          }
        }
      }
    }
  }

  private String getNextEndpoint() {
    int idx = counter.getAndUpdate(i -> (i + 1) % s3gEndpoints.size());
    return s3gEndpoints.get(idx);
  }

  private boolean isHopByHopHeader(String header) {
    String lowerName = header.toLowerCase();
    return "connection".equals(lowerName) || "keep-alive".equals(lowerName)
        || "proxy-authenticate".equals(lowerName) || "proxy-authorization".equals(lowerName)
        || "te".equals(lowerName) || "trailers".equals(lowerName)
        || "transfer-encoding".equals(lowerName) || "upgrade".equals(lowerName);
  }
}
