package com.brightlysoftware.testingkafka;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.smallrye.reactive.messaging.providers.connectors.InMemoryConnector;

import java.util.HashMap;
import java.util.Map;


public class KafkaTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    @Override
    public Map<String, String> start() {
        Map<String, String> env = new HashMap<>();
        Map<String, String> props1 = InMemoryConnector.switchIncomingChannelsToInMemory("orders");
        Map<String, String> props2 = InMemoryConnector.switchOutgoingChannelsToInMemory("beverages");
        env.putAll(props1);
        env.putAll(props2);
        return env;
    }

    @Override
    public void stop() {
        InMemoryConnector.clear();
    }
}
