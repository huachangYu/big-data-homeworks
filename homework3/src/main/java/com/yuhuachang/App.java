package com.yuhuachang;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

public class App {
    public static void main(String[] args) {
        HbaseController hbaseController = new HbaseController();
        hbaseController.init();
        String[] names = {"basic", "info"};
        hbaseController.createTable("user", names);
        hbaseController.addDataRow("user", "00001", "basic", "id", "10001");
        hbaseController.addDataRow("user", "00001", "info", "name", "yuhuachang");
        Cell[] cells = hbaseController.getDataRow("user", "00001");
        for (Cell cell : cells) {
            String rowKey = new String(CellUtil.cloneRow(cell));
            String family = new String(CellUtil.cloneFamily(cell));
            String columnKey = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));
            System.out.println(rowKey + " " + family + " " + columnKey + " " + value);
        }
    }
}
