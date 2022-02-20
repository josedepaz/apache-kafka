package application.plain;

import application.helper.Constants;
import domain.HealthCheck;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class PlainProcessor {
    private Consumer<String, String> consumer;

    private Producer<String, String> producer;

    public PlainProcessor(String brokers) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", brokers);
        consumerProps.put("group.id", "healthcheck-processor");
        consumerProps.put("key.deserializer", StringDeserializer.class);
        consumerProps.put("value.deserializer", StringDeserializer.class);
        consumer = new KafkaConsumer<>(consumerProps);

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", brokers);
        producerProps.put("key.serializer", StringSerializer.class);
        producerProps.put("value.serializer", StringSerializer.class);
        producer = new KafkaProducer<>(producerProps);
    }


        public final void process() {
            consumer.subscribe(Collections.singletonList(
                    Constants.getHealthChecksTopic()));           //1
            while(true) {
                ConsumerRecords records = consumer.poll(Duration.ofSeconds(1L)); //2
                for(Object record : records) {                //3
                    ConsumerRecord it = (ConsumerRecord) record;
                    String healthCheckJson = (String) it.value();
                    HealthCheck healthCheck = null;
                    try {
                        healthCheck = Constants.getJsonMapper()
                                .readValue(healthCheckJson, HealthCheck.class);     // 4
                    } catch (IOException e) {
                        // deal with the exception
                    }
                    LocalDate startDateLocal =healthCheck.getLastStartedAt().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();        //5
                    int uptime =
                            Period.between(startDateLocal, LocalDate.now()).getDays();  //6
                    Future future =
                            producer.send(new ProducerRecord<>(
                                    Constants.getUptimesTopic(),
                                    healthCheck.getSerialNumber(),
                                    String.valueOf(uptime)));                  //7
                    try {
                        future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        // deal with the exception
                    }
                }
            }
        }
}
