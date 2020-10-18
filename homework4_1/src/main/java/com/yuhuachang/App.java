package com.yuhuachang;

import redis.clients.jedis.Jedis;

public class App {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("120.79.149.10", 6379);
        // 插入字符串
        jedis.set("0106160126", "yuhuachang");
        
        //插入列表List
        jedis.rpush("animal", "dog");
        jedis.rpush("animal", "cat");
        jedis.rpush("animal", "lion");

        //插入集合Set
        jedis.sadd("vegetables", "tomato");
        jedis.sadd("vegetables", "potato");
        jedis.sadd("vegetables", "carota");
        jedis.sadd("vegetables", "carota");

        //查询
        System.out.println(jedis.get("0106160126"));
        System.out.println(jedis.lrange("animal", 0, 10));
        System.out.println(jedis.smembers("vegetables"));

        //删除
        jedis.del("0106160126");
        jedis.del("animal");
        jedis.del("vegetables");

        // 关闭redis
        jedis.close();
    }
}
