package kafka.extractor;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GeoIPService {

  private static final String MAXMINDDB = "/Users/josedepaz/Documents/Temp/GeoLiteCity.dat";

  public Location getLocation(String ipAddress) {
    try {
      final LookupService maxmind =
          new LookupService(MAXMINDDB, LookupService.GEOIP_MEMORY_CACHE);
      return maxmind.getLocation(ipAddress);
    } catch (IOException ex) {
      Logger.getLogger(GeoIPService.class.getName()).log(Level.SEVERE, null, ex);
    }
    return null;
  }
}
