package application.custom;

import application.helper.Constants;
import application.serde.HealthCheckSerializer;
import com.github.javafaker.Faker;
import domain.HealthCheck;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class CustomProducer {
    private final Producer<String, HealthCheck> producer;
    public CustomProducer(String brokers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);                    //1
        props.put("key.serializer", StringSerializer.class);        //2
        props.put("value.serializer", HealthCheckSerializer.class); //3
        producer = new KafkaProducer<>(props);                      //4
    }

    public void produce(int ratePerSecond) {
        long waitTimeBetweenIterationsMs = 1000L / (long)ratePerSecond; //1
        Faker faker = new Faker();
        while(true) { //2
            HealthCheck fakeHealthCheck    =       new HealthCheck(
                    "HEALTH_CHECK",
                    faker.address().city(),                    //1
                    faker.bothify("??##-??##", true),    //2
                    Constants.machineType.values()
                            [faker.number().numberBetween(0,4)].toString(), //3
                    Constants.machineStatus.values()
                            [faker.number().numberBetween(0,3)].toString(), //4
                    faker.date().past(100, TimeUnit.DAYS),           //5
                    faker.number().numberBetween(100L, 0L),          //6
                    faker.internet().ipV4Address());
            Future futureResult = producer.send( new ProducerRecord<>(
                    Constants.getHealthChecksTopic(), fakeHealthCheck));       //3
            try {
                Thread.sleep(waitTimeBetweenIterationsMs); //4
                futureResult.get();      //5
            } catch (InterruptedException | ExecutionException e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
