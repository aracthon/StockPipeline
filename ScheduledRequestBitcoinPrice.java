package com.individual.bitcoin;

import com.coinbase.api.Coinbase;
import com.coinbase.api.CoinbaseBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.money.CurrencyUnit;
import org.joda.money.Money;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimerTask;

public class ScheduledRequestBitcoinPrice extends TimerTask {

    private Producer<String, String> producer;
    private String topicName;

    public ScheduledRequestBitcoinPrice(Producer<String, String> producer,
                                        String topicName) {
        this.producer = producer;
        this.topicName = topicName;
    }

    public void run() {
        simpleProducer(producer, topicName);
    }

    private void simpleProducer(Producer<String, String> producer, String topicName) {
        System.out.println("Start to fetch Bitcoin price");
        try {
            Coinbase cb = new CoinbaseBuilder()
                    .withApiKey(System.getenv("COINBASE_API_KEY"), System.getenv("COINBASE_API_SECRET"))
                    .build();
            Money spotPrice = cb.getSpotPrice(CurrencyUnit.USD);

            Calendar cal = Calendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

            producer.send(
                    new ProducerRecord<String, String>(
                            topicName,
                            sdf.format(cal.getTime()),
                            spotPrice.toString()
                    )
            );
            System.out.println("Successfully sent Bitcoin price" + spotPrice.toString() + "to Kafka");
        } catch (Exception e) {
            System.out.println("Failed to send Bitcoin price to Kafka");
        }
    }
}