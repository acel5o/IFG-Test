package org.ifg;

import org.ifg.service.KafkaConsumerService;

import javax.inject.Inject;

public class Main {

    @Inject
    static KafkaConsumerService consumerService;

    public static void main(String[] args) {
        System.out.println("Starting async Kafka consumer...");
        consumerService.run();
    }
}
