package com.brightlysoftware.testingkafka;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.reactive.messaging.providers.connectors.InMemoryConnector;
import io.smallrye.reactive.messaging.providers.connectors.InMemorySink;
import io.smallrye.reactive.messaging.providers.connectors.InMemorySource;
import org.apache.kafka.common.protocol.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.enterprise.inject.Any;
import javax.inject.Inject;
import java.util.List;

@QuarkusTest
@QuarkusTestResource(KafkaTestResourceLifecycleManager.class)
public class BaristaTest {

    @Inject
    @Any
    InMemoryConnector connector;

    @Test
    void testProcessOrder() throws InterruptedException {

        // InMemoryConnector connector= new InMemoryConnector();
        //conn.
        InMemorySource<Order> ordersIn = connector.source("orders");
        InMemorySink<Beverage> beveragesOut = connector.sink("beverages");

        Order order = new Order();
        order.setProduct("coffee");
        order.setCustomer("Coffee lover");
        order.setOrderId("1234");

        ordersIn.send(order);


       // () -> wait()

       // wait().<List<? extends Message>>

                //until(beveragesOut::received, t -> t.size() == 1);




        Beverage queuedBeverage = beveragesOut.received().get(0).getPayload();
        Assertions.assertEquals("RECEIVED", queuedBeverage.getPreparationState());
        Assertions.assertEquals("coffee", queuedBeverage.getDrink());
        Assertions.assertEquals("Coffee lover", queuedBeverage.getCustomer());
        Assertions.assertEquals("1234", queuedBeverage.getOrderId());
    }
}
