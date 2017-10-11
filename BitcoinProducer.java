package com.individual.bitcoin;

import java.util.Properties;
import java.util.Timer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;


public class BitcoinProducer {

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            System.out.println("Enter topic name as argument");
        }

        // parse the argument
        String topicName = args[0].trim().toString();

        // configure system properties
        Properties props = new Properties();
        BitcoinProducer bp = new BitcoinProducer();
        bp.configureProperties(props);

        // declare producer
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        // schedule task: request Bitcoin price then send to kafka every 1 sec
        Timer time = new Timer();
        ScheduledRequestBitcoinPrice st = new ScheduledRequestBitcoinPrice(producer, topicName);
        time.schedule(st, 0, 1000);

        // register shutdown hook
        ShutdownHook sample = new ShutdownHook(producer);
        sample.attachShutDownHook();

        while (true);
    }

    private void configureProperties(Properties props) throws Exception {
        if (props == null) {
            throw new Exception();
        }

        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("bath.size", 32768);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }
}