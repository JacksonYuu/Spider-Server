package com.itcast.dao;

import com.itcast.tools.ReadFile;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.bson.Document;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * 将HBase里的数据进行统计
 */
public class HBaseStatistics {

    //日志打印类
    private Logger logger = LoggerFactory.getLogger(getClass());

    //配置信息路径
    private String path;

    //读取后的信息
    private String file;

    //信息传化
    private JSONObject jsonObject;

    //实例化对象
    private MongoDBStorage mongoDBStorage = MongoDBStorage.getInstance();

    //得到mongoClient
    private MongoClient mongoClient = mongoDBStorage.init();

    //集合管理
    private MongoCollection<Document> collections;

    //实例化对象
    private HBaseStorage hBaseStorage = HBaseStorage.getInstance();

    //数据表管理类
    private Table table;

    /**
     * 构造方法
     */
    public static HBaseStatistics instance;

    public static synchronized HBaseStatistics getInstance() {
        if (instance == null) {
            instance = new HBaseStatistics();
        }
        return instance;
    }

    /**
     * 运行将会进行统计
     * @param args
     */
    public static void main(String[] args) {
        try {

            HBaseStatistics hBaseStatistics = HBaseStatistics.getInstance();

            hBaseStatistics.hBaseStorage.init();

            hBaseStatistics.countProvinceDistribution("province");

            hBaseStatistics.countEducationDistribution("education");

            hBaseStatistics.close();
        } catch (Exception e) {

            e.printStackTrace();
        }
    }

    /**
     * 解析配置文件信息并创建行上遍历器
     * @param message 文件名称
     * @return 行上遍历器
     */
    public ResultScanner analyseConfiguration(String message) throws Exception {

        //读取配置文件信息
        path = System.getProperty("user.dir") + "/configuration/job_" + message + ".json";

        file = ReadFile.readFile(path);

        jsonObject = new JSONObject(file);

        //取出配置文件信息
        String tablename = jsonObject.getString("hbase_tablename");

        String family = jsonObject.getString("hbase_family");

        String[] columns = jsonObject.getString("hbase_column").split(",");

        String database = jsonObject.getString("mongodb_database");

        String collection = jsonObject.getString("mongodb_collection");

        //创建集合连接
        collections = mongoDBStorage.createCollection(mongoClient, database, collection);

        //创建对数据表管理类
        table = hBaseStorage.connTable(hBaseStorage, TableName.valueOf(tablename));

        //这只查询的条件
        Scan scan = new Scan();

        for (String column : columns) {

            scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        }

        scan.setCaching(60);

        scan.setMaxResultSize(1 * 1024 * 1024);

        //获取行上遍历器
        return table.getScanner(scan);
    }

    /**
     * 关闭各种连接
     * @throws Exception
     */
    public void close() throws Exception {

        if (table != null) {

            table.close();
        }

        if (hBaseStorage != null) {

            hBaseStorage.close();
        }

        if (mongoDBStorage != null) {

            mongoDBStorage.close();
        }
    }

    /**
     * 统计岗位地区的分布
     * @param message 名称信息
     */
    public void countProvinceDistribution(String message) throws Exception {

        ResultScanner resultScanner = analyseConfiguration(message);

        String[] allMessage = jsonObject.getString(message).split(",");

        int length = allMessage.length;

        //存放总共招聘人数
        int[] cu = new int[length];

        //存放岗位的地域数量
        int[] nu = new int[length];

        //创建文档存放信息
        Document[] documents = new Document[length];

        //存放文档列表
        List<Document> documentList = new ArrayList<>();

        for (Result result : resultScanner) {

            //得到岗位地点
            String loc = (new String(CellUtil.cloneValue(result.rawCells()[1]))).split("-")[0];

            String t = new String(CellUtil.cloneValue(result.rawCells()[0]));

            if (t.equals("")) {

                t = "0";
            }

            //得到岗位招聘数量
            int total = Integer.valueOf(t);

            for (int i = 0; i < length; i++) {

                documents[i] = new Document();

                //得到省份存在的地域数组
                String[] pro = jsonObject.getString(allMessage[i]).split(",");

                int num = 1;

                if (Arrays.asList(pro).contains(loc)) {

                    cu[i] += total;

                    nu[i] += num;
                }
            }
        }

        for (int i = 0; i < length; i++) {

            logger.debug(i + " = " + cu[i] + " : " + nu[i] + " : " + allMessage[i]);

            documents[i].append("name", allMessage[i]).append("amount", cu[i]).append("number", nu[i]);

            documentList.add(documents[i]);
        }

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date da = new Date();

        String date = simpleDateFormat.format(da);

        Document document = new Document();

        document.append("date", date).append("category", documentList);

        collections.insertOne(document);

        logger.debug(message + " Collection Insert Successful!");
    }

    /**
     * 岗位学历分布
     * @param message
     * @throws Exception
     */
    public void countEducationDistribution(String message) throws Exception {

        ResultScanner resultScanner = analyseConfiguration(message);

        String[] allMessage = jsonObject.getString(message).split(",");

        Integer length = allMessage.length;

        //存放总共招聘人数
        int[] cu = new int[length];

        //存放岗位的地域数量
        int[] nu = new int[length];

        //创建文档存放信息
        Document[] documents = new Document[length];

        //存放文档列表
        List<Document> documentList = new ArrayList<>();

        for (Result result : resultScanner) {

            String edu = new String(CellUtil.cloneValue(result.rawCells()[1]));

            String t = new String(CellUtil.cloneValue(result.rawCells()[0]));

            if (t.equals("")) {

                t = "0";
            }

            int total = Integer.valueOf(t);

            int num = 1;

            for (int i = 0; i < length; i++) {

                documents[i] = new Document();

                if (allMessage[i].equals(edu)) {

                    cu[i] = cu[i] + total;

                    nu[i] = nu[i] + num;
                }
            }
        }

        for (int i = 0; i < length; i++) {

            logger.debug(i + " = " + cu[i] + " : " + nu[i] + " : " + allMessage[i]);

            documents[i].append("type", allMessage[i]).append("amount", cu[i]).append("number", nu[i]);

            documentList.add(documents[i]);
        }

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date da = new Date();

        String date = simpleDateFormat.format(da);

        Document document = new Document();

        document.append("date", date).append("category", documentList);

        collections.insertOne(document);

        logger.debug(message + " Collection Insert Successful!");
    }
}
