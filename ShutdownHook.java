package com.individual.bitcoin;

import org.apache.kafka.clients.producer.Producer;

public class ShutdownHook {
    Producer<String, String> producer;
    public ShutdownHook(Producer<String, String> producer) {
        this.producer = producer;
    }

    public ShutdownHook() {

    }

    public void attachShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    producer.flush();
                    System.out.println("Finished flushing pending messages");
                } finally {
                    try {
                        producer.close();
                        System.out.println("Kafka connection closed");
                    } catch (Exception e) {
                        System.out.println("Failed to close Kafka connection");
                    }
                }
            }
        });
    }
}