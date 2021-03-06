package kafka;

import kafka.producer.Enricher;
import kafka.reader.Reader;
import kafka.validator.Validator;
import kafka.writer.Writer;

public class MainApplication {

  public static void main0(String[] args) {

    System.out.println("hola");
    String servers = args[0];
    String groupId = args[1];
    String sourceTopic = args[2];
    String targetTopic = args[3];
    Reader reader = new Reader(servers, groupId, sourceTopic);
    Writer writer = new Writer(servers, targetTopic);
    reader.run(writer);

  }

  public static void main(String[] args) {

    try {
      String servers = args[0];
      String groupId = args[1];
      String inputTopic = args[2];
      String validTopic = args[3];
      String invalidTopic = args[4];
      for(String arg: args) {
        System.out.println("arg: " + arg);
      }
      Reader reader = new Reader(servers, groupId, inputTopic);
      Enricher enricher = new Enricher(servers, validTopic, invalidTopic);
      reader.run(enricher);

    } catch(Exception ex) {
      System.out.println("There are not enough args to run..");
    }

  }

}
