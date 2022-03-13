package application.custom;

import application.helper.Constants;
import domain.HealthCheck;
import org.apache.kafka.common.serialization.Deserializer;
import java.io.IOException;
import java.util.Map;

public final class HealthCheckDeserializer implements Deserializer {

    @Override
    public HealthCheck deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return Constants.getJsonMapper().readValue(data, HealthCheck.class);
        } catch (IOException e) {
            return null;
        }
    }
    @Override
    public void close() {}
    @Override
    public void configure(Map configs, boolean isKey) {}
}
