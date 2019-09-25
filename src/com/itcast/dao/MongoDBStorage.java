package com.itcast.dao;

import com.itcast.tools.ReadFile;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * mongodb存储类
 */
public class MongoDBStorage {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private MongoClient mongoClient = null;

    /**
     * 构造方法
     */
    public static MongoDBStorage instance;

    public static synchronized MongoDBStorage getInstance() {
        if (instance == null) {
            instance = new MongoDBStorage();
        }
        return instance;
    }

    /**
     * 解析配置信息
     * @return
     */
    public HashMap<String, String> analyseConfiguration() {

        HashMap<String, String> map = new HashMap<>();

        //读取配置文件信息
        String path = System.getProperty("user.dir") + "/configuration/mongodb.json";

        String file = ReadFile.readFile(path);

        JSONObject jsonObject = new JSONObject(file);

        //添加信息
        map.put("server_ip", jsonObject.getString("server_ip"));

        map.put("server_port", jsonObject.getString("server_port"));

        //返回信息
        return map;
    }

    /**
     * 初始化连接
     * @return 连接
     */
    public MongoClient init() {

        //得到配置信息
        MongoDBStorage mongoDBStorage = MongoDBStorage.getInstance();

        HashMap<String, String> map = mongoDBStorage.analyseConfiguration();

        String ip = map.get("server_ip");

        Integer port = Integer.parseInt(map.get("server_port"));

        //创建mongodb数据库的连接
        mongoClient = new MongoClient(ip, port);

        logger.debug("MongoDB Connection Successful!");

        return mongoClient;
    }

    /**
     * 关闭连接
     */
    public void close() {

        if (mongoClient != null) {

            mongoClient.close();
        }
    }

    /**
     * 创建集合并连接
     * @param mongoClient 数据库连接
     * @param database 数据库名称
     * @param collection 集合名称
     * @return
     */
    public MongoCollection<Document> createCollection(MongoClient mongoClient, String database, String collection) {
        try {

            //连接到数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(database);

            logger.debug("MongoDB Database Connection Successful!");

            //连接到集合
            MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

            logger.debug("MongoDB Collection Connection Successful!");

            return mongoCollection;
        } catch (Exception e) {

            logger.debug("Not found here!");

            logger.error(e.toString());

            return null;
        }
    }

    /**
     * 连接数据库和集合
     * @param mongoClient 连接数据库
     * @param database 数据库名称
     * @param collection 集合名称
     * @return MongoCursor<Document> 游标
     */
    public MongoCursor<Document> connectCollection(MongoClient mongoClient, String database, String collection) {
        try {

            //连接到数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(database);

            logger.debug("MongoDB Database Connection Successful!");

            //连接到集合
            MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

            logger.debug("MongoDB Collection Connection Successful!");

            //获取迭代器
            FindIterable<Document> findIterable = mongoCollection.find();

            //获取游标
            MongoCursor<Document> mongoCursor = findIterable.iterator();

            return mongoCursor;
        } catch (Exception e) {

            logger.debug("Not found here!");

            logger.error(e.toString());

            return null;
        }
    }

    /**
     * 在数据库中找到地点的父城市编码
     * @param mongoClient 连接类
     * @param database 数据库
     * @param collection 集合
     * @param num 判断寻找父城市的范围
     * @param location 地点或编码
     * @return 父城市的编码
     */
    public Object getParentCityCode(MongoClient mongoClient, String database, String collection, Integer num, String location) {
        try {

            MongoCursor<Document> mongoCursor;

            MongoDBStorage mongoDBStorage = MongoDBStorage.getInstance();

            //得到游标器
            mongoCursor = mongoDBStorage.connectCollection(mongoClient, database, collection);

            while (mongoCursor.hasNext()) {

                Document document = mongoCursor.next();

                //由于每个文档都是一个编号，所以用了循环得到每个文档
                for (int i = 0; i < num; i++) {

                    Document doc = (Document) document.get("" + i);

                    String name = (String) doc.get("name");

                    String code = (String) doc.get("code");

                    Boolean term;

                    if (num > 2000) {

                        term = name.contains(location);
                    } else {

                        term = code.equals(location);
                    }

                    if (term) {

                        Object parent_code = doc.get("parent_code");

                        return parent_code;
                    }
                }
            }
        } catch (Exception e) {

            logger.debug("Not found here!");

            logger.error(e.toString());

            return null;
        }

        return null;
    }

    /**
     * 在数据库中找到编码所在的省份名称
     * @param mongoClient 连接类
     * @param database 数据库
     * @param collection 集合
     * @param num 为了循环用
     * @param parent_code 父城市编码
     * @return 父城市的名称
     */
    public String getParentCityName(MongoClient mongoClient, String database, String collection, Integer num, Object parent_code) {
        try {

            MongoDBStorage mongoDBStorage = MongoDBStorage.getInstance();

            MongoCursor<Document> mongoCursor;

            //获取游标器
            mongoCursor = mongoDBStorage.connectCollection(mongoClient, database, collection);

            while (mongoCursor.hasNext()) {

                Document document = mongoCursor.next();

                //由于每个文档都是一个编号，所以用了循环得到每个文档
                for (int i = 0; i < num; i++) {

                    Document doc = (Document) document.get("" + i);

                    String name = (String) doc.get("name");

                    if (parent_code.equals(doc.get("code"))) {

                        return name;
                    }
                }
            }
        } catch (Exception e) {

            logger.debug("Not found here!");

            logger.error(e.toString());

            return null;
        }

        return null;
    }
}
