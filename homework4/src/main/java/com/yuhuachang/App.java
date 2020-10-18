package com.yuhuachang;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;

import org.bson.Document;

public class App {
    public static void main(String[] args) throws Exception {
        MongoController mongoController = new MongoController();
        
        // 建立连接
        mongoController.open();

        // 插入数据
        // Document dom = new Document();
        // dom.put("name", "jack");
        // dom.put("sid", "22051107");
        // dom.put("age", 22);
        // mongoController.insert("people", "student", dom);

        // 查询数据
        // mongoController.findAll("people", "student");
        // BasicDBObject query = new BasicDBObject();
        // List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
        // obj.add(new BasicDBObject("age", 21));
        // query.append("$and", obj);
        // mongoController.find(query, "people", "student");

        // 数据更新
        BasicDBObject filterQuery = new BasicDBObject();
        filterQuery.put("name", "jack");
        BasicDBObject updateQeury = new BasicDBObject();
        BasicDBObject res = new BasicDBObject();
        res.put("name", "jackson");
        updateQeury.put("$set", res);
        mongoController.update(filterQuery, updateQeury, "people", "student");
        mongoController.findAll("people", "student");
        mongoController.close();
    }
}
