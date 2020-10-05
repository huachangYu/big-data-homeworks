package com.yuhuachang;

public class App {
    public static void main(String[] args) {
        HbaseController hbaseController = new HbaseController();
        hbaseController.init();
        String[] names = {"id", "info"};
        hbaseController.createTable("java", names);
    }
}
