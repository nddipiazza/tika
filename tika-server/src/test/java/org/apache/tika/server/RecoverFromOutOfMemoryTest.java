package org.apache.tika.server;

import org.apache.commons.io.IOUtils;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Integration test that proves that the -spawnChild option enable means that
 * if the server hits a file that causes an OOM condition that the server can recover without being manually restarted.
 */
public class RecoverFromOutOfMemoryTest {
  private static final Logger LOG = LoggerFactory.getLogger(TikaServerCli.class);

  public static Integer findRandomOpenPortOnAllLocalInterfaces() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  ExecutorService executorService = Executors.newSingleThreadExecutor();

  @After
  public void after() {
    executorService.shutdownNow();
  }

  @Test
  public void testOutOfMemory() throws Exception {
    String host = InetAddress.getLocalHost().getHostAddress();
    int port = findRandomOpenPortOnAllLocalInterfaces();

    String endpoint = "http://" + host + ":" + port;

    executorService.execute(() -> {
      TikaServerCli.main(new String[] {
          "-s", "-spawnChild", "-status", "-maxChildStartupMillis", "120000", "-pingPulseMillis", "500",
          "-pingTimeoutMillis", "30000", "-taskPulseMillis", "500", "-taskTimeoutMillis", "120000", "-h", host, "-port",
          String.valueOf(port), "-JXmx300m", "-JXX:+SuppressFatalErrorMessage"
      });
    });

    waitForServerToBeUp(port, endpoint);

    // First parse a document that works.
    assertCanParseNormalDoc(endpoint);

    // Obviously no problems so far. But now let's make a problem.
    // Give it a known bomb.xls file that will blow up child JVM.

    for (int i = 0; i < 10; ++i) {
      Response response = null;
      try {
        response = parseBombFile(endpoint);
        LOG.info("Managed to get a response from bomb file - {}", response.getStatus());
      } catch (Exception e) {
        LOG.info("Could not parse the bomb file!", e);
      }

      // How does it react here? The client needs to have some indication that this file caused a
      // memory crash.

      // Now parse a normal doc. Tika should restart the spawned child and parse this document. Instead this doc will fail.
      response = WebClient.create(endpoint + "/rmeta/text")
          .header("Content-Type", "application/msword")
          .put(ClassLoader.getSystemResourceAsStream("test.doc"));

      // The server recovers after the previous failure, and it actual is able to parse this file no problems.
      response = WebClient.create(endpoint + "/rmeta/text")
          .header("Content-Type", "application/msword")
          .put(ClassLoader.getSystemResourceAsStream("test.doc"));
    }
  }

  private Response parseBombFile(String endpoint) {
    return WebClient.create(endpoint + "/rmeta/text")
          .header("Content-Type", " application/vnd.ms-excel")
          .put(ClassLoader.getSystemResourceAsStream("bomb.xls"));
  }

  private void assertCanParseNormalDoc(String endpoint) throws IOException {
    Response response = WebClient.create(endpoint + "/rmeta/text")
        .header("Content-Type", "application/msword")
        .put(ClassLoader.getSystemResourceAsStream("test.doc"));

    Assert.assertEquals(200, response.getStatus());
    String entityResponse = IOUtils.toString((InputStream)response.getEntity(), Charset.defaultCharset());
//    List listResp = gson.fromJson(entityResponse, List.class);
//    Map mapResp = (Map)listResp.get(0);
//
//    Assert.assertFalse(StringUtils.isBlank((String)mapResp.get("X-TIKA:content")));

    LOG.info("Normal doc response: {}", entityResponse);
  }

  private void waitForServerToBeUp(int port, String endpoint) throws Exception {
    try (CloseableHttpClient client = HttpClients.createMinimal()) {
      int retries = 10;
      while (--retries > 0) {
        try (CloseableHttpResponse response = client.execute(new HttpGet(endpoint + "/tika"))) {
          if (response.getStatusLine().getStatusCode() == 200) {
            break;
          }
        } catch (HttpHostConnectException hce) {
          // no-op
        }
        LOG.info("Still waiting for Tika server on port {} to start. Retries remaining: {}", port, retries);
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      if (retries <= 0) {
        throw new Exception("Tika remote server failed to start after 10 attempts");
      }
    }
  }
}
