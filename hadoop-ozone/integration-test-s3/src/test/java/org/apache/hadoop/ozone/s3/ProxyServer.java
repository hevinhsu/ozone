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
import java.util.Optional;
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

        HttpURLConnection conn = createConnection(exchange, target);
        String requestMethod = conn.getRequestMethod();

        LOG.info("Received request and forwarding request to [{}] {}", requestMethod, target);
        LOG.info("Request headers:");
        exchange.getRequestHeaders().forEach((key, value) -> {
          LOG.info("  {} : {}", key, value);
        });

        copyRequestHeaders(exchange, conn);

        copyRequestBody(exchange, conn);

        int responseCode = conn.getResponseCode();

        copyResponseHeaders(exchange, conn);

        if(requestMethod.equals("HEAD") || responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
          exchange.sendResponseHeaders(responseCode, -1);
          conn.disconnect();
          return;
        }

        copyResponseBody(exchange, conn, responseCode);

      } catch (Exception e) {
        LOG.error("Error forwarding request to S3G endpoint", e);
        exchange.sendResponseHeaders(502, 0);
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

    private void copyResponseBody(HttpExchange exchange, HttpURLConnection conn, int responseCode) throws IOException {
      Integer responseContentLength = Optional.ofNullable(conn.getHeaderField("Content-Length"))
          .map(Integer::parseInt)
          .orElse(0);
      exchange.sendResponseHeaders(responseCode, responseContentLength);

      try (InputStream is = responseCode >= 400 ? conn.getErrorStream() : conn.getInputStream();
           OutputStream os = exchange.getResponseBody()) {
        if (is != null) {
          IOUtils.copy(is, os);
        }
      }
    }

    private void copyResponseHeaders(HttpExchange exchange, HttpURLConnection conn) {
      // copy response headers
      for (String headerName : conn.getHeaderFields().keySet()) {
        if (headerName != null && !isHopByHopHeader(headerName)) {
          List<String> headerValues = conn.getHeaderFields().get(headerName);
          for (String headerValue : headerValues) {
            exchange.getResponseHeaders().add(headerName, headerValue);
          }
        }
      }
    }

    private HttpURLConnection createConnection(HttpExchange exchange, String target) throws IOException {
      HttpURLConnection conn = (HttpURLConnection) new URL(target).openConnection();
      String requestMethod = exchange.getRequestMethod();
      conn.setRequestMethod(requestMethod);
      return conn;
    }

    private void copyRequestBody(HttpExchange exchange, HttpURLConnection conn) throws IOException {
      List<String> contentLengths = exchange.getRequestHeaders().get("Content-Length");
      if (contentLengths != null) {
        conn.setFixedLengthStreamingMode(Integer.parseInt(contentLengths.get(0)));
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
          IOUtils.copy(exchange.getRequestBody(), os);
        }
      }
    }

    private void copyRequestHeaders(HttpExchange exchange, HttpURLConnection conn) {
      for (String headerName : exchange.getRequestHeaders().keySet()) {
        if (isHopByHopHeader(headerName)) {
          continue;
        }

        List<String> headerValues = exchange.getRequestHeaders().get(headerName);
        if (headerValues != null && !headerValues.isEmpty()) {
          String joined = String.join(",", headerValues);
          // httpServer will convert first character to upper case.
          // so we convert header name to small case to avoid header name mismatch in s3g
          conn.setRequestProperty(headerName.toLowerCase(), joined);
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
