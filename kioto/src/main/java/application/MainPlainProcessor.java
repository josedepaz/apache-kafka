package application;

import application.plain.PlainProcessor;

public class MainPlainProcessor {

    public static void main(String[] args) {
        new PlainProcessor("localhost:9092").process(); //7
    }
}
