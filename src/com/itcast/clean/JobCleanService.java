package com.itcast.clean;

import com.itcast.dao.HBaseStorage;
import com.itcast.dao.JobDataRepository;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

/**
 * 清洗数据
 */
public class JobCleanService {

    //日志打印类
    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 构造方法
     */
    public static JobCleanService instance;

    public static synchronized JobCleanService getInstance() {
        if (instance == null) {
            instance = new JobCleanService();
        }
        return instance;
    }

    /**
     * 清洗数据
     * @param tablename1 查询的数据表
     * @param tablename2 存入的数据表
     * @param family1 查询数据表的列族名称
     * @param family2 存入数据表的列族名称
     */
    public void cleanJobMessage(String tablename1, String tablename2, String family1, String family2) {
        try {

            logger.debug("Start cleaning data into the " + tablename2);

            //存储信息
            HashMap<String, Object> map = new HashMap<>();

            //连接HBase数据库
            HBaseStorage hBaseStorage = HBaseStorage.getInstance();

            hBaseStorage.init();

            //连接到数据表
            Table getTable = hBaseStorage.connTable(hBaseStorage, TableName.valueOf(tablename1));

            //数据查询类
            Scan scan = new Scan();

            scan.addFamily(Bytes.toBytes(family1));

            //取得所有数据
            ResultScanner resultScanner = getTable.getScanner(scan);

            //遍历数据
            for (Result result : resultScanner) {

                List<Cell> listCells = result.listCells();

                for (Cell cell : listCells) {

                    //得到列族
                    String f = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());

                    //得到列
                    String c = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

                    //得到值
                    String v = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

                    logger.debug("列族:" + f + " 列:" + c + " 值:" + v);

                    //开始清洗数据
                    switch (c) {

                        case "date" :

                            v = JobCleanUtils.cleanAmount(v);

                            map.put("date", v);

                            break;

                        case "amount" :

                            v = JobCleanUtils.cleanAmount(v);

                            map.put("amount", v);

                            break;

                        case "jobName" :

                            map.put("jobName", v);

                            break;

                        case "category" :

                            map.put("category", v);

                            break;

                        case "company" :

                            map.put("company", v);

                            break;

                        case "description" :

                            map.put("description", v);

                            break;

                        case "education" :

                            map.put("education", v);

                            break;

                        case "experience" :

                            v = String.valueOf(JobCleanUtils.cleanExperience(v));

                            map.put("experience", v);

                            break;

                        case "id" :

                            map.put("id", v);

                            break;

                        case "industry" :

                            map.put("industry", v);

                            break;

                        case "keywords" :

                            map.put("keywords", v);

                            break;

                        case "location" :

                            v = JobCleanUtils.cleanLocation(v);

                            map.put("location", v);

                            break;

                        case "nature" :

                            map.put("nature", v);

                            break;

                        case "salary" :

                            v = String.valueOf(JobCleanUtils.cleanSalary(v));

                            map.put("salary", v);

                            break;

                        case "scale" :

                            v = JobCleanUtils.cleanScale(v);

                            map.put("scale", v);

                            break;
                    }

                    logger.debug("列族:" + f + " 列:" + c + " 值:" + v);
                }

                //保存数据
                JobDataRepository jobDataRepository = JobDataRepository.getInstance();

                jobDataRepository.insertDataToHBase(hBaseStorage, tablename2, family2, map);
            }

            hBaseStorage.close();
        } catch (Exception e) {

            logger.debug("There is a problem here!");

            logger.debug(e.toString());
        }
    }
}
