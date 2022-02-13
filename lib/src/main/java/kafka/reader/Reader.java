package kafka.reader;

import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import kafka.consumer.Consumer;
import kafka.producer.Producer;

public class Reader implements Consumer {
  private final KafkaConsumer<String, String> consumer;//1
  private final String topic;

  public Reader(String servers, String groupId, String topic) {
    this.consumer =
        new KafkaConsumer<>(Consumer.createConfig(servers, groupId));
    this.topic = topic;
  }

  public void run(Producer producer) {
    this.consumer.subscribe(Collections.singletonList(this.topic));//2
    while (true) {//3
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));  //4
      for (ConsumerRecord<String, String> record : records) {
        producer.process(record.value());//5
      }
    }
  }
}
