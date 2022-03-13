package application.consumer;

import application.custom.HealthCheckDeserializer;
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

import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class CustomProcessor {

    private Consumer<String, HealthCheck> consumer;
    private Producer<String, String> producer;

    public CustomProcessor(String brokers) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", brokers);
        consumerProps.put("group.id", "healthcheck-processor");
        consumerProps.put("key.deserializer", StringDeserializer.class);
        consumerProps.put("value.deserializer", HealthCheckDeserializer.class);
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
            for(Object record : records) {                 //3
                try {
                ConsumerRecord it = (ConsumerRecord) record;
                HealthCheck healthCheck = (HealthCheck) it.value(); //4
                LocalDate startDateLocal =healthCheck.getLastStartedAt().toInstant()
                        .atZone(ZoneId.systemDefault()).toLocalDate();         //5
                int uptime =
                        Period.between(startDateLocal, LocalDate.now()).getDays();  //6
                Future future =
                        producer.send(new ProducerRecord<>(
                                Constants.getUptimesTopic(),
                                healthCheck.getSerialNumber(),
                                String.valueOf(uptime)));                  //7

                    future.get();
                } catch (InterruptedException | NullPointerException | ExecutionException e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }

}
