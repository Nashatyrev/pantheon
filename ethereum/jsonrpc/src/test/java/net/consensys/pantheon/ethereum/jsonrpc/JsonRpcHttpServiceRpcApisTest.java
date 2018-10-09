package net.consensys.pantheon.ethereum.jsonrpc;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import net.consensys.pantheon.ethereum.blockcreation.MiningCoordinator;
import net.consensys.pantheon.ethereum.core.Synchronizer;
import net.consensys.pantheon.ethereum.core.TransactionPool;
import net.consensys.pantheon.ethereum.eth.EthProtocol;
import net.consensys.pantheon.ethereum.jsonrpc.JsonRpcConfiguration.RpcApis;
import net.consensys.pantheon.ethereum.jsonrpc.internal.filter.FilterManager;
import net.consensys.pantheon.ethereum.jsonrpc.internal.methods.JsonRpcMethod;
import net.consensys.pantheon.ethereum.jsonrpc.internal.queries.BlockchainQueries;
import net.consensys.pantheon.ethereum.jsonrpc.internal.response.JsonRpcError;
import net.consensys.pantheon.ethereum.mainnet.MainnetProtocolSchedule;
import net.consensys.pantheon.ethereum.p2p.api.P2PNetwork;
import net.consensys.pantheon.ethereum.p2p.wire.Capability;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JsonRpcHttpServiceRpcApisTest {

  private final Vertx vertx = Vertx.vertx();
  private final OkHttpClient client = new OkHttpClient();
  private JsonRpcHttpService service;
  private static String baseUrl;
  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private static final String CLIENT_VERSION = "TestClientVersion/0.1.0";
  private static final String NET_VERSION = "6986785976597";
  private JsonRpcConfiguration configuration;

  @Mock protected static BlockchainQueries blockchainQueries;

  private final JsonRpcTestHelper testHelper = new JsonRpcTestHelper();

  @Before
  public void before() {
    configuration = JsonRpcConfiguration.createDefault();
    configuration.setPort(0);
  }

  @After
  public void after() {
    service.stop().join();
  }

  @Test
  public void requestWithNetMethodShouldSucceedWhenDefaultApisEnabled() throws Exception {
    service = createJsonRpcHttpServiceWithRpcApis(configuration);
    final String id = "123";
    final RequestBody body =
        RequestBody.create(
            JSON,
            "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(id) + ",\"method\":\"net_version\"}");
    final Request request = new Request.Builder().post(body).url(baseUrl).build();

    try (Response resp = client.newCall(request).execute()) {
      assertThat(resp.code()).isEqualTo(200);
    }
  }

  @Test
  public void requestWithNetMethodShouldSucceedWhenNetApiIsEnabled() throws Exception {
    service = createJsonRpcHttpServiceWithRpcApis(RpcApis.NET);
    final String id = "123";
    final RequestBody body =
        RequestBody.create(
            JSON,
            "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(id) + ",\"method\":\"net_version\"}");
    final Request request = new Request.Builder().post(body).url(baseUrl).build();

    try (Response resp = client.newCall(request).execute()) {
      assertThat(resp.code()).isEqualTo(200);
    }
  }

  @Test
  public void requestWithNetMethodShouldFailWhenNetApiIsNotEnabled() throws Exception {
    service = createJsonRpcHttpServiceWithRpcApis(RpcApis.WEB3);
    final String id = "123";
    final RequestBody body =
        RequestBody.create(
            JSON,
            "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(id) + ",\"method\":\"net_version\"}");
    final Request request = new Request.Builder().post(body).url(baseUrl).build();

    try (Response resp = client.newCall(request).execute()) {
      assertThat(resp.code()).isEqualTo(400);
      // Check general format of result
      final JsonObject json = new JsonObject(resp.body().string());
      final JsonRpcError expectedError = JsonRpcError.METHOD_NOT_FOUND;
      testHelper.assertValidJsonRpcError(
          json, id, expectedError.getCode(), expectedError.getMessage());
    }
  }

  @Test
  public void requestWithNetMethodShouldSucceedWhenNetApiAndOtherIsEnabled() throws Exception {
    service = createJsonRpcHttpServiceWithRpcApis(RpcApis.NET, RpcApis.WEB3);
    final String id = "123";
    final RequestBody body =
        RequestBody.create(
            JSON,
            "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(id) + ",\"method\":\"net_version\"}");
    final Request request = new Request.Builder().post(body).url(baseUrl).build();

    try (Response resp = client.newCall(request).execute()) {
      assertThat(resp.code()).isEqualTo(200);
    }
  }

  private JsonRpcConfiguration createJsonRpcConfigurationWithRpcApis(final RpcApis... rpcApis) {
    final JsonRpcConfiguration config = JsonRpcConfiguration.createDefault();
    config.setCorsAllowedDomains(singletonList("*"));
    config.setPort(0);
    if (rpcApis != null) {
      config.setRpcApis(Lists.newArrayList(rpcApis));
    }
    return config;
  }

  private JsonRpcHttpService createJsonRpcHttpServiceWithRpcApis(final RpcApis... rpcApis) {
    return createJsonRpcHttpServiceWithRpcApis(createJsonRpcConfigurationWithRpcApis(rpcApis));
  }

  private JsonRpcHttpService createJsonRpcHttpServiceWithRpcApis(
      final JsonRpcConfiguration config) {
    final Set<Capability> supportedCapabilities = new HashSet<>();
    supportedCapabilities.add(EthProtocol.ETH62);
    supportedCapabilities.add(EthProtocol.ETH63);

    final Map<String, JsonRpcMethod> rpcMethods =
        spy(
            new JsonRpcMethodsFactory()
                .methods(
                    CLIENT_VERSION,
                    NET_VERSION,
                    mock(P2PNetwork.class),
                    blockchainQueries,
                    mock(Synchronizer.class),
                    MainnetProtocolSchedule.create(),
                    mock(FilterManager.class),
                    mock(TransactionPool.class),
                    mock(MiningCoordinator.class),
                    supportedCapabilities,
                    config.getRpcApis()));
    final JsonRpcHttpService jsonRpcHttpService = new JsonRpcHttpService(vertx, config, rpcMethods);
    jsonRpcHttpService.start().join();

    baseUrl = jsonRpcHttpService.url();
    return jsonRpcHttpService;
  }
}