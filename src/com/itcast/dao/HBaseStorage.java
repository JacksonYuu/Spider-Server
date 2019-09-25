package com.itcast.dao;

import com.itcast.tools.ReadFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * HBase存储类
 */
public class HBaseStorage {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Configuration configuration;

    private Connection connection;

    private Admin admin;

    //设置配置文件的路径
    private String path = System.getProperty("user.dir") + "/configuration/hbase.json";

    //读取路径文件信息
    private String hbaseMessage = ReadFile.readFile(path);

    //将文件信息格式转化为JSON格式
    private JSONObject jsonObject = new JSONObject(hbaseMessage);

    /**
     * 构造方法
     */
    public static HBaseStorage instance;

    public static synchronized HBaseStorage getInstance() {
        if (instance == null) {
            instance = new HBaseStorage();
        }
        return instance;
    }

    /**
     * 运行这个类将会创建配置信息中hbase数据表
     * @param args
     */
    public static void main(String[] args) {

        //构造类
        HBaseStorage hBaseStorage = HBaseStorage.getInstance();

        //初始化连接
        hBaseStorage.init();

        //创建数据表
        hBaseStorage.createTable(hBaseStorage);

        //关闭连接
        hBaseStorage.close();
    }

    /**
     * 连接数据表
     * @param tableName 数据表名称
     * @return 数据表管理类
     */
    public Table connTable(HBaseStorage hBaseStorage, TableName tableName) {
        try {

            Table table = connection.getTable(tableName);

            logger.debug(tableName + " Connection Successful!");

            return table;
        } catch (IOException e) {

            logger.debug(tableName + " Connection Fail!");

            return null;
        }
    }

    /**
     * 创建表
     */
    public void createTable(HBaseStorage hBaseStorage) {

        //得到需要创建的表信息
        HashMap<String, String[]> map = hBaseStorage.analyseConfiguration();

        //遍历HashMap得到表信息和对应列族信息
        for (Map.Entry<String, String[]> m : map.entrySet()) {

            String tablename = m.getKey();

            String[] colFamily = m.getValue();

            //将String类型转化为表类型
            TableName tableName = TableName.valueOf(tablename);

            try {

                //判断在hbase数据库中是否存在相同名称的表
                if (!admin.tableExists(tableName)) {

                    //创建数据表的描述信息类
                    HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

                    for (int i = 0; i < colFamily.length; i++) {

                        //创建列族的描述信息类
                        HColumnDescriptor columnDescriptor = new HColumnDescriptor(colFamily[i]);

                        //将列族信息加入表信息中
                        tableDescriptor.addFamily(columnDescriptor);
                    }

                    //根据数据表描述信息创建数据表
                    admin.createTable(tableDescriptor);

                    logger.debug("HBase Create " + tablename + " Successful!");
                } else {

                    logger.debug(tablename + " already exist!");
                }
            } catch (Exception e) {

                logger.debug("HBase Create " + tablename + " Fail!");

                logger.debug(e.toString());
            }
        }
    }

    /**
     * 解析配置信息
     * @return 表名称和对应的列族信息
     */
    public HashMap<String, String[]> analyseConfiguration() {

        HashMap<String, String[]> map = new HashMap<>();

        //得到需要创建表的名称
        String[] tablenames = jsonObject.getString("table_name").split(",");

        //得到所有表的列族
        JSONArray colFamilies = jsonObject.getJSONArray("colFamilies");

        JSONObject colFamily = colFamilies.getJSONObject(0);

        //将表名称和对应的列族信息保存
        for (int i = 0; i < tablenames.length; i++) {

            map.put(tablenames[i], colFamily.getString(String.valueOf(i + 1)).split(","));
        }

        //返回保存的信息
        return map;
    }

    /**
     * 初始化连接
     */
    public void init() {

        //创建hbase数据库的配置信息类
        configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.quorum", jsonObject.getString("server_ip"));

        configuration.set("hbase.zookeeper.property.clientPort", jsonObject.getString("server_port"));

        configuration.set("zookeeper.znode.parent", jsonObject.getString("parent_path"));

        try {
            //创建hbase数据库的连接
            connection = ConnectionFactory.createConnection(configuration);

            //得到hbase数据库元数据的连接
            admin = connection.getAdmin();

            logger.debug("HBase Connection Successful!");
        } catch (IOException e) {

            logger.debug("HBase Connection Fail!");

            logger.debug(e.toString());
        }
    }

    /**
     * 关闭各种连接
     */
    public void close() {
        if (admin != null) try {
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (connection != null) try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.debug("HBase Connection Close!");
    }
}
