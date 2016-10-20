package org.windwant.consul;

import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;
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
 * Created by windwant on 2016/8/18.
 */
public class ConsulMgr {

    private static KeyValueClient keyValueClient;
    private static HealthClient healthClient;
    private static AgentClient agentClient;
    static {
        Consul consul = Consul.builder().withHostAndPort(HostAndPort.fromParts("192.168.7.162", 8500)).build();
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
        ImmutableRegCheck immutableRegCheck = ImmutableRegCheck.builder().tcp("192.168.7.162:22").interval("5s").build();

        ImmutableRegistration.Builder builder = ImmutableRegistration.builder();
        ImmutableRegistration immutableRegistration = builder.
                id("jetty").
                name("jettySvr").
                addTags("container").
                address("192.168.7.162").
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

    public static void main(String[] args) {
        monitor();
    }
}
