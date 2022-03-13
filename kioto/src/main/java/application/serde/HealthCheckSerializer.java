package application.serde;

import application.helper.Constants;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class HealthCheckSerializer implements Serializer {
    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        try {
            return Constants.getJsonMapper().writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    @Override
    public void close() {}
    @Override
    public void configure(Map configs, boolean isKey) {}
}
