package kafka.producer;

import java.io.IOException;
import org.apache.kafka.clients.producer.KafkaProducer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.maxmind.geoip.Location;
import kafka.extractor.GeoIPService;

public class Enricher implements Producer{
  private final KafkaProducer<String, String> producer;
  private final String validMessages;
  private final String invalidMessages;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  public Enricher(String servers, String validMessages, String
      invalidMessages) {
    this.producer = new KafkaProducer<>
        (Producer.createConfig(servers));
    this.validMessages = validMessages;
    this.invalidMessages = invalidMessages;
  }
  @Override
  public void process(String message) {
    try {
      final JsonNode root = MAPPER.readTree(message);
      final JsonNode ipAddressNode =
          root.path("customer").path("ipAddress");
      if (ipAddressNode.isMissingNode()) {
        Producer.write(this.producer, this.invalidMessages,
            "{\"error\": \"customer.ipAddress is missing\"}");
      } else {
        final String ipAddress = ipAddressNode.textValue();
        final Location location = new GeoIPService().getLocation(ipAddress);
        ((ObjectNode) root).with("customer").put("country",
            location.countryName);
        ((ObjectNode) root).with("customer").put("city",
            location.city);
        Producer.write(this.producer, this.validMessages,
            MAPPER.writeValueAsString(root));
      }
    } catch (IOException ex) {
      Producer.write(this.producer, this.invalidMessages, "{\"error\": \""
          + ex.getClass().getSimpleName() + ": " + ex.getMessage() + "\"}");
    }
  }
}
