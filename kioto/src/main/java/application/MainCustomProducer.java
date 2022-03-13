package application;

import application.custom.CustomProducer;

public class MainCustomProducer {

    public static void main(String[] args) {
        new CustomProducer("localhost:9092").produce(2);
    }
}
