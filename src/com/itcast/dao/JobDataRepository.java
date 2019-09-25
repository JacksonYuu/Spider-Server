package com.itcast.dao;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 存储job信息
 */
public class JobDataRepository {

    //日志打印类
    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 构造方法
     */
    public static JobDataRepository instance;

    public static synchronized JobDataRepository getInstance() {
        if (instance == null) {
            instance = new JobDataRepository();
        }
        return instance;
    }

    /**
     * 将数据添加进入HBase数据库
     * @param tablename 数据表名称
     * @param family 列族名称
     * @param allJobMessage 所有的数据
     */
    public void insertDataToHBase(HBaseStorage hBaseStorage, String tablename, String family, HashMap<String, Object> allJobMessage) {
        try {

            logger.debug("Start inserting data into the " + tablename);

            TableName tableName = TableName.valueOf(tablename);

            //创建对于表数据管理的类
            Table table = hBaseStorage.connTable(hBaseStorage, tableName);

            //数据添加列表
            List<Put> putList = new ArrayList<>();

            //创建对于单元格数据输入管理的类
            Put put;

            //向表中添加数据
            if (allJobMessage.get("id") != null) {

                put = new Put(Bytes.toBytes((String) allJobMessage.get("id")));

                for (Map.Entry m : allJobMessage.entrySet()) {

                    String key = (String) m.getKey();

                    String value = (String) m.getValue();

                    logger.debug("列族:" + family + " 列:" + key + " 值:" + value);

                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(value));
                }

                putList.add(put);
            }

            //执行所有的put，将数据写入表中
            table.put(putList);

            //关闭对数据表的连接
            table.close();

            logger.debug("Ends inserting data into the " + tablename);
        } catch (IOException e) {

            logger.debug("There is a problem here!");

            logger.debug(e.toString());
        }
    }

    /**
     * 根据数据表和列族查询指定的列所有数据
     * @param tablename 数据表
     * @param family 列族
     * @param column 指定列
     * @return 查询到所有的数据列表
     */
    public List<String> queryDataByColumn(HBaseStorage hBaseStorage, String tablename, String family,
                                          String column) {

        List<String> message = new ArrayList<>();

        try {

            logger.debug("Start querying the " + tablename + " for data");

            TableName tableName = TableName.valueOf(tablename);

            //创建对于表数据管理类
            Table table = hBaseStorage.connTable(hBaseStorage, tableName);

            //设置查询的一些参数
            Scan scan = new Scan();

            //scan.setCaching(60);//设定抓取得时间

            //scan.setMaxResultSize(1 * 1024 * 1024);//设置结果最大容量

            //scan.setFilter(new PageFilter(100));//设置最大的查询数量

            //添加查询的列族和具体的列
            scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));

            //取得表中指定列族指定列的所有数据
            ResultScanner resultScanner = table.getScanner(scan);

            for (Result result : resultScanner) {

                List<Cell> listCells = result.listCells();

                for (Cell cell : listCells) {

                    //得到数据的列族
                    //String fam = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());

                    //得到数据的列
                    //String col = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

                    //得到数据的值
                    String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

                    message.add(val);
                }
            }

            //关闭连接
            table.close();

            logger.debug("Ends querying the " + tablename + " for data!");
        } catch (Exception e) {

            logger.debug("There is a problem here!");

            logger.debug(e.toString());
        }

        return message;
    }

    public List<String> queryDataByCondition(HBaseStorage hBaseStorage, String tablename, String family,
                                             String column, List<String> conditions, String column2) {

        List<String> message = new ArrayList<>();

        try {

            logger.debug("Start querying the " + tablename + " for data");

            //连接到数据表
            Table table = hBaseStorage.connTable(hBaseStorage, TableName.valueOf(tablename));

            for (int i = 0; i < conditions.size(); i++) {

                String con = conditions.get(i);

                Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column),
                        CompareFilter.CompareOp.EQUAL, Bytes.toBytes(con));

                Scan scan = new Scan();

                scan.setFilter(filter);

                ResultScanner resultScanner = table.getScanner(scan);

                for (Result result : resultScanner) {

                    List<Cell> cellList = result.listCells();

                    for (Cell cell : cellList) {

                        String f = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());

                        String c = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

                        String v = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

                        if (c.equals(column2)) {

                            message.add(v);
                        }
                    }
                }
            }

            //关闭连接
            table.close();

            logger.debug("Ends querying the " + tablename + " for data!");
        } catch (Exception e) {

            logger.debug("There is a problem here!");

            logger.error(e.toString());
        }

        return message;
    }
}
