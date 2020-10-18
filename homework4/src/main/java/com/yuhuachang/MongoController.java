package com.yuhuachang;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;

import org.bson.Document;

public class MongoController {
    MongoClient client = null;
    
    //连接
    public void open() {
        client = new MongoClient("120.79.149.10", 27017);
    }

    // 关闭
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    //插入数据
    public void insert(String databaseName, String collectionName, Document document) {
        client.getDatabase(databaseName).getCollection(collectionName).insertOne(document);
    }

    //创建集合
    public void createCollection(String databaseName, String name) {
        client.getDatabase(databaseName).createCollection(name);
    }

    //显示所有数据
    public void findAll(String databaseName, String name) {
        MongoCursor<Document> cursor = client.getDatabase(databaseName).getCollection(name).find().cursor();
        while(cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    //查询数据
    public void find(BasicDBObject query, String databaseName, String name) {
        MongoCursor<Document> cursor = client.getDatabase(databaseName).getCollection(name).find(query).cursor();
        while(cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    public void update(BasicDBObject filter, BasicDBObject query, String databaseName, String name) {
        client.getDatabase(databaseName).getCollection(name).updateMany(filter, query);
    }
}
