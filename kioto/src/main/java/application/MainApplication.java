package application;

import application.producer.PlainProducer;

public class MainApplication {

  public static void main0(String[] args) {
    new PlainProducer("localhost:9092").produce(2); //7
  }

  public static void main(String[] args) {
    new PlainProducer("localhost:9092").produce(2); //7
  }
}
