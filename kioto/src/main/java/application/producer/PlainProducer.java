package application.producer;

import application.helper.Constants;
import com.fasterxml.jackson.core.JsonProcessingException;

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

public final class PlainProducer {
  private final Producer<String, String> producer;

  public PlainProducer(String brokers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokers);                //1
    props.put("key.serializer", StringSerializer.class);    //2
    props.put("value.serializer", StringSerializer.class);  //3
    producer = new KafkaProducer<>(props);                  //4
  }

  public void produce(int ratePerSecond) {
    long waitTimeBetweenIterationsMs = 3000L / (long)ratePerSecond; //1
    Faker faker = new Faker();

    while(true) { //2
      HealthCheck fakeHealthCheck =
          new HealthCheck(
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
      String fakeHealthCheckJson = null;
      try {
        fakeHealthCheckJson = Constants.getJsonMapper().writeValueAsString(fakeHealthCheck); //3
      } catch (JsonProcessingException e) {
        // deal with the exception
      }
      Future futureResult = producer.send(new ProducerRecord<>
          (Constants.getHealthChecksTopic(), fakeHealthCheckJson)); //4
      try {
        Thread.sleep(waitTimeBetweenIterationsMs); //5
        futureResult.get(); //6
        System.out.println(fakeHealthCheckJson);
      } catch (InterruptedException | ExecutionException e) {
        // deal with the exception
      }
    }

  }

}
