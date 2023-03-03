package com.brightlysoftware.testingkafka;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BeverageProcessor {

    @Incoming("orders")
    @Outgoing("beverages")
    Beverage process(Order order) {
        System.out.println("Order received " + order.getProduct());
        Beverage beverage = new Beverage();
        beverage.setDrink(order.getProduct());
        beverage.setCustomer(order.getCustomer());
        beverage.setOrderId(order.getOrderId());
        beverage.setPreparationState("RECEIVED");
        return beverage;
    }

}