package com.yuhuachang;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseController {
    private Configuration conf;
    private Connection connection;

    public void init() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://ubuntu133:9000/hbase");
        conf.set("hbase.zookeeper.quorum", "ubuntu133");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createTable(String name, String[] familyNames) {
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(name));
        for (String familyName : familyNames) {
            tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName)).build());
        }
        try {
            connection.getAdmin().createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void addDataRow(String tableName, String rowKey, String family, String columnKey, String value) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put puts = new Put(Bytes.toBytes(rowKey));
            for (ColumnFamilyDescriptor columnFamilyDescriptor : table.getDescriptor().getColumnFamilies()) {
                if (columnFamilyDescriptor.getNameAsString().equals(family)) {
                    puts.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnKey), Bytes.toBytes(value));
                    break;
                }
            }
            table.put(puts);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteDataRow(String tableName, String rowKey) {
        Table table;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Cell[] getDataRow(String tableName, String rowKey) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Cell[] cells = table.get(new Get(Bytes.toBytes(rowKey))).rawCells();
            table.close();
            return cells;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void open() {

    }

    public void close() {
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
