package com.yuhuachang;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class WriteToHbase {
    public static void main(String[] args) throws IOException {
        HbaseController hbaseController = new HbaseController();
        hbaseController.init();
        String[] names = {"name", "info"};
        hbaseController.createTable("province", names);
        FileInputStream inputStream = new FileInputStream("output-province/part-r-00000");
        InputStreamReader reader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line = "";
        while ((line = bufferedReader.readLine()) != null) {
            String[] cells = line.split("\t");
            hbaseController.addDataRow("province", cells[0], "name", "name", cells[0]);
            hbaseController.addDataRow("province", cells[0], "info", "cnt", cells[1]);
        }
        bufferedReader.close();
    }
}
