package application;

import application.consumer.CustomProcessor;

public class MainCustomProcessor {

    public static void main( String[] args) {
        new CustomProcessor("localhost:9092").process();
    }
}
