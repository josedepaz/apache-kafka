package application.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class PlainConsumer {
    private Consumer<String, String> consumer;
    public PlainConsumer(String brokers) {
        Properties props = new Properties();
        props.put("group.id", "healthcheck-processor");         //1
        props.put("bootstrap.servers", brokers);                   //2
        props.put("key.deserializer", StringDeserializer.class);   //3
        props.put("value.deserializer", StringDeserializer.class); //4
        consumer = new KafkaConsumer<>(props);                        //5
    }
}
