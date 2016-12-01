package org.windwant.consul;

import com.ecwid.consul.json.GsonFactory;
import com.ecwid.consul.transport.RawResponse;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.ConsulRawClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.health.model.HealthService;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.status.StatusConsulClient;
import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;
import com.google.gson.reflect.TypeToken;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.async.ConsulResponseCallback;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.agent.ImmutableRegCheck;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.health.Node;
import com.orbitz.consul.model.health.Service;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.option.QueryOptions;

import java.math.BigInteger;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * consul.exe agent -server -bootstrap -data-dir=data -bind=127.0.0.1 -client 0.0.0.0 -ui
 * Created by windwant on 2016/8/18.
 */
public class ConsulMgr {

    private static KeyValueClient keyValueClient;
    private static HealthClient healthClient;
    private static AgentClient agentClient;
    static {
        Consul consul = Consul.builder()
                .withConnectTimeoutMillis(3000)
                .withPing(true)
                .withReadTimeoutMillis(2000)
                .withWriteTimeoutMillis(2000)
                .withHostAndPort(HostAndPort.fromParts("127.0.0.1", 8500)).build();
        keyValueClient = consul.keyValueClient();
        healthClient = consul.healthClient();
        agentClient = consul.agentClient();
    }

    public static void put(String key, String value){
        keyValueClient.putValue(key, value);
    }

    public static String getValueAsString(String key){
        return keyValueClient.getValueAsString(key).toString();
    }

    /**
     * 获取正常服务
     * @param serviceName
     */
    public static void getHealthService(String serviceName){
        List<ServiceHealth> nodes = healthClient.getHealthyServiceInstances(serviceName).getResponse();
        System.out.println(nodes);
        System.out.println(nodes.size());
        nodes.forEach((resp) -> {
            Node node = resp.getNode();
            System.out.println("node: " + node.getNode());
            System.out.println("address: " + node.getAddress());
            Service service = resp.getService();
            System.out.println("service: " + service.getService());
            System.out.println("service id: " + service.getId());
            System.out.println("service port: " + service.getPort());
            System.out.println("service address: " + service.getAddress());
            System.out.println("service tags: " + service.getTags());
        });


    }

    /**
     * 注册服务
     */
    public static void registerService(){
        ImmutableRegCheck immutableRegCheck = ImmutableRegCheck.builder().tcp("119.254.118.122:8089").interval("5s").build();

        ImmutableRegistration immutableRegistration = ImmutableRegistration.builder().
                id("tomcat").
                name("tomcatSvr").
                addTags("container").
                address("119.254.118.122").
                port(8089).
                addChecks(immutableRegCheck).
                build();

        agentClient.register(immutableRegistration);
    }

    /**
     * 监听
     */
    public static void monitor(){
        final ConsulResponseCallback<Optional<Value>> callback = new ConsulResponseCallback<Optional<Value>>() {

            AtomicReference<BigInteger> index = new AtomicReference<BigInteger>(null);

            public void onComplete(ConsulResponse<Optional<Value>> consulResponse) {

                if (consulResponse.getResponse().isPresent()) {
                    Value v = consulResponse.getResponse().get();
                    System.out.println(String.format("Value is: %s", new String(Base64.getDecoder().decode(v.getValue().get()))));
                }
                index.set(consulResponse.getIndex());
                watch();
            }

            void watch() {
                keyValueClient.getValue("student", QueryOptions.blockSeconds(5, index.get()).build(), this);
            }

            public void onFailure(Throwable throwable) {
//                System.out.println("Error encountered");
//                watch();
            }
        };

        keyValueClient.getValue("student", QueryOptions.blockSeconds(5, new BigInteger("0")).build(), callback);
    }

    public static void monitorX(){
        keyValueClient.getValue("student", QueryOptions.blockSeconds(5, new BigInteger("0")).build(), new ConsulResponseCallback<Optional<Value>>() {
            AtomicReference<BigInteger> index = new AtomicReference<BigInteger>(null);
            public void onComplete(ConsulResponse<Optional<Value>> consulResponse) {
                if (consulResponse.getResponse().isPresent()) {
                    Value v = consulResponse.getResponse().get();
                    System.out.println(String.format("Value is: %s", new String(Base64.getDecoder().decode(v.getValue().get()))));
                }
                index.set(consulResponse.getIndex());
                keyValueClient.getValue("student", QueryOptions.blockSeconds(1, index.get()).build(), this);
            }

            public void onFailure(Throwable throwable) {
                System.out.println(throwable.getMessage());
            }
        });
    }

    public static void mianClient(){
        ConsulClient consulClient = new ConsulClient("127.0.0.1", 8500);
        Response<GetValue> response = consulClient.getKVValue("student");
        System.out.println(new String(Base64.getDecoder().decode(response.getValue().getValue())));
    }

    //ROOT API kv health status
    public static void rawClient(){
        ConsulRawClient rawClient = new ConsulRawClient("127.0.0.1", 8500);
        RawResponse rawResponse = rawClient.makeGetRequest("/v1/kv/student");
        List<GetValue> list = GsonFactory.getGson().fromJson(rawResponse.getContent(),
                new TypeToken<List<GetValue>>() {
                }.getType());
        GetValue getValue = list.get(0);
        System.out.println(getValue.getKey());
        System.out.println(new String(Base64.getDecoder().decode(getValue.getValue())));

        rawResponse = rawClient.makeGetRequest("/v1/health/service/consul");
        List<HealthService> slist = GsonFactory.getGson().fromJson(rawResponse.getContent(),
                new TypeToken<List<HealthService>>() {
                }.getType());
        HealthService hservice = slist.get(0);
        System.out.println(hservice.getService().toString());

        rawResponse = rawClient.makeGetRequest("/v1/agent/services");
        com.ecwid.consul.v1.agent.model.Service aservice = GsonFactory.getGson().fromJson(rawResponse.getContent(),
                new TypeToken<com.ecwid.consul.v1.agent.model.Service>() {
                }.getType());
        System.out.println(aservice.toString());

        rawResponse = rawClient.makeGetRequest("/v1/status/leader");
        String leaders = rawResponse.getContent();
        System.out.println(leaders);
    }

    public static void statusClient(){
        StatusConsulClient statusClient = new StatusConsulClient("127.0.0.1", 8500);
        Response leader = statusClient.getStatusLeader();
        Response peer = statusClient.getStatusPeers();
        System.out.println(leader.toString());
        System.out.println(peer.toString());
    }

    public static void check(){
        agentClient.register(8089, HostAndPort.fromParts("119.254.118.122", 8089), 1, "122", "122svr");
    }

    public static void main(String[] args) {
        monitorX();
    }
}
