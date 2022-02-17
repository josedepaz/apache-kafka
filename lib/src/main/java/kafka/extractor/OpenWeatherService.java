package kafka.extractor;

import java.io.IOException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class OpenWeatherService {

  private static final String API_KEY = "e17599c57e9c76b63d10f83661779630"; //1
  private static final ObjectMapper MAPPER = new ObjectMapper();
  public double getTemperature(String lat, String lon) {
    try {
      final URL url = new URL(
          "http://api.openweathermap.org/data/2.5/weather?lat=" + lat             + "&lon="+ lon +
              "&units=metric&appid=" + API_KEY); //2
      final JsonNode root = MAPPER.readTree(url);
      final JsonNode node = root.path("main").path("temp");
      return Double.parseDouble(node.toString());
    } catch (IOException ex) {
      Logger.getLogger(OpenWeatherService.class.getName()).log(Level.SEVERE, null, ex);
    }
    return 0;
  }
}
