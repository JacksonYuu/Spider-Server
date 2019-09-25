package com.itcast.cluster;

import com.itcast.dao.HBaseStorage;
import com.itcast.dao.JobDataRepository;
import com.itcast.dao.MongoDBStorage;
import com.itcast.tools.ReadFile;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class JobClusterService {

    //日志打印类
    private Logger logger = LoggerFactory.getLogger(getClass());

    //配置文件信息位置
    private String path = System.getProperty("user.dir") + "/configuration/job_cluster_config.json";

    //读取文件信息
    private String file = ReadFile.readFile(path);

    //解析文件信息
    private JSONObject jsonObject = new JSONObject(file);

    /**
     * 构造方法
     */
    public static JobClusterService instance;

    public static synchronized JobClusterService getInstance() {
        if (instance == null) {
            instance = new JobClusterService();
        }
        return instance;
    }

    /**
     * 将岗位信息进行聚类
     * @param clusterN 需要聚类的数量
     */
    public void clusterJobMessage(Integer clusterN) {

        //数据表名称,列族名称
        String tablename = null, family = null;

        //列名称,分类名称,关键字
        String[] columns = null, categorys = null, skills = null;

        //读取配置文件信息
        JSONArray totals = jsonObject.getJSONArray("total");

        for (int i = 0; i < totals.length(); i++) {

            JSONObject total = totals.getJSONObject(i);

            JSONArray industrys = total.getJSONArray("industry");

            for (int j = 0; j < industrys.length(); j++) {

                JSONObject industry = industrys.getJSONObject(j);

                tablename = industry.getString("tablename");

                family = industry.getString("family");

                columns = industry.getString("column").split(",");

                categorys = industry.getString("category").split(",");

                skills = industry.getString("skills").split(",");
            }
        }

        //构造类
        HBaseStorage hBaseStorage = HBaseStorage.getInstance();

        //初始化连接HBase数据库
        hBaseStorage.init();

        //构造类
        MongoDBStorage mongoDBStorage = MongoDBStorage.getInstance();

        //初始化连接MongoDB数据库
        MongoClient mongoClient = mongoDBStorage.init();

        //构造类
        JobDataRepository jobDataRepository = JobDataRepository.getInstance();

        JobClusterUtils jobClusterUtils = new JobClusterUtils();

        //得到配置信息中所有列的信息
        List<String> jobName = jobDataRepository.queryDataByColumn(hBaseStorage, tablename, family, columns[0]);

        List<String> id = jobDataRepository.queryDataByColumn(hBaseStorage, tablename, family, columns[1]);

        //得到分类之后的岗位信息
        HashMap<String, HashMap<String, String>> allJobs = jobClusterUtils.allJObClassification(id, jobName);

        //循环取得需要的岗位信息
        for (String category : categorys) {

            HashMap<String, String> allJob = allJobs.get(category);

            List<String> secondId = new ArrayList<>();

            List<String> secondJobName = new ArrayList<>();

            //将岗位id和jonName取出
            for (Map.Entry<String, String> m : allJob.entrySet()) {

                secondId.add(m.getKey());

                secondJobName.add(m.getValue());
            }

            //根据分类之后的信息找到描述信息
            List<String> description = jobDataRepository.queryDataByCondition(hBaseStorage, tablename, family, columns[1], secondId, columns[2]);

            //得到描述信息向量
            List<double[]> weights = jobClusterUtils.descriptionToWeights(description, skills);

            //得到清洗后的描述的位置信息
            List<Integer> index = jobClusterUtils.cleanWeight(weights);

            //保存清洗后的岗位信息
            List<String> lastId = new ArrayList<>();

            List<String> lastJobName = new ArrayList<>();

            List<double[]> lastWeights = new ArrayList<>();

            for (int i = 0; i < index.size(); i++) {

                Integer indexof = index.get(i);

                lastId.add(secondId.get(indexof));

                lastJobName.add(secondJobName.get(indexof));

                lastWeights.add(weights.get(indexof));
            }

            //获取当前时间
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            Date da = new Date();

            String date = simpleDateFormat.format(da);

            //创建集合
            MongoCollection<Document> collection = mongoDBStorage.createCollection(mongoClient, tablename, tablename + "_" + category);

            //最外面的文档
            Document document = new Document();

            document.append("date", date).append("count", clusterN);

            //里面一层的文档
            List<Document> document1 = new ArrayList<>();

            //聚类的位置信息
            List<Integer> integerList = jobClusterUtils.KMeans(lastWeights, clusterN);

            for (int i = 0; i < clusterN; i++) {

                //保存聚类后的岗位信息
                List<String> finalId = new ArrayList<>();

                List<String> finalJobName = new ArrayList<>();

                for (int j = 0; j < integerList.size(); j++) {

                    if (integerList.get(j).equals(i)) {

                        finalId.add(lastId.get(j));

                        finalJobName.add(lastJobName.get(j));
                    }
                }

                logger.debug("cluster " + (i + 1) + " 共有:" + finalId.size());

                Document doc = new Document();

                doc.append("name", finalJobName.get(0)).append("amount", finalJobName.size());

                document1.add(doc);
            }

            document.append("category", document1);

            collection.insertOne(document);
        }

        //关闭连接
        mongoDBStorage.close();

        hBaseStorage.close();
    }
}
