package com.itcast.cluster;

import com.itcast.tools.ReadFile;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 岗位聚类具体操作
 */
public class JobClusterUtils {

    //日志打印类
    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 构造方法
     */
    public static JobClusterUtils instance;

    public static synchronized JobClusterUtils getInstance() {
        if (instance == null) {
            instance = new JobClusterUtils();
        }
        return instance;
    }

    /**
     * Kmeans算法（调用spark的）
     * @param weights 向量
     * @param clusterN 聚类数量
     * @return 聚类后的位置
     */
    public List<Integer> KMeans(List<double[]> weights, Integer clusterN) {

        List<Integer> index = new ArrayList<>();

        List<Vector> vectors = new ArrayList<>();

        //转化为向量
        for (int i = 0; i < weights.size(); i++) {

            vectors.add(Vectors.dense(weights.get(i)));
        }

        //创建对于spark的连接
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("job_cluster");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        //将向量转化为spark的rdd类型
        JavaRDD<Vector> distance = sparkContext.parallelize(vectors);

        //调用spark写好的KMeans方法
        KMeansModel model = KMeans.train(distance.rdd(), clusterN, 5000, 300);

        JavaRDD<Integer> position = model.predict(distance);

        //将其结果保存
        for (int i = 0; i < position.collect().size(); i++) {

            index.add(position.collect().get(i));
        }

        //关闭连接
        sparkContext.close();

        return index;
    }

    /**
     * 将描述信息转化为词频向量
     * @param description 描述信息
     * @param skills 关键字
     * @return 向量列表
     */
    public List<double[]> descriptionToWeights (List<String> description, String[] skills) {

        List<double[]> weights = new ArrayList<>();

        Integer length = skills.length;

        //循环描述
        for (String d : description) {

            double[] weight = new double[length];

            //循环关键字
            for (int i = 0; i < length; i++) {

                if (d.contains(skills[i])) {

                    weight[i] = 1;
                } else {

                    weight[i] = 0;
                }
            }

            weights.add(weight);
        }

        return weights;
    }

    /**
     *  清洗描述信息中关键字小于5的信息
     * @param weights 描述信息
     * @return 描述信息的位置
     */
    public List<Integer> cleanWeight(List<double[]> weights) {

        List<Integer> index = new ArrayList<>();

        for (int i = 0; i < weights.size(); i++) {

            double sum = 0;

            double[] weight = weights.get(i);

            for (int j = 0; j < weight.length; j++) {

                sum = sum + weight[j];
            }

            if (sum > 5) {

                index.add(i);
            }
        }

        return index;
    }

    /**
     * 将所有信息进行分类
     * @param id 岗位id列表
     * @param jobName 岗位名称列表
     * @return 分类之后的信息
     */
    public HashMap<String, HashMap<String, String>> allJObClassification(List<String> id, List<String> jobName) {

        HashMap<String, HashMap<String, String>> allJobs = new HashMap<>();

        //将查询的信息以id和jobName对应起来
        HashMap<String, String> allJob = new HashMap<>();

        for (int i = 0; i < id.size(); i++) {

            allJob.put(id.get(i), jobName.get(i));
        }

        //配置文件信息位置
        String path = System.getProperty("user.dir") + "/configuration/job_cluster_config.json";

        //读取文件信息
        String file = ReadFile.readFile(path);

        //解析文件信息
        JSONObject jsonObject = new JSONObject(file);

        JSONArray totals = jsonObject.getJSONArray("total");

        //保存配置信息
        HashMap<String, Object> map = new HashMap<>();

        for (int i = 0; i < totals.length(); i++) {

            JSONObject total = totals.getJSONObject(i);

            JSONArray classifications = total.getJSONArray("classification");

            for (int j = 0; j < classifications.length(); j++) {

                JSONObject classification = classifications.getJSONObject(j);

                String category = classification.getString("category");

                String[] keywords = classification.getString("keywords").split(",");

                map.put(category, keywords);
            }
        }

        //将jobName按照category进行分类
        for (Map.Entry<String, Object> m : map.entrySet()) {

            String key = m.getKey();

            allJobs.put(key, new HashMap<>());

            String[] value = (String[]) m.getValue();

            for (Map.Entry<String, String> ma : allJob.entrySet()) {

                for (String s : value) {

                    if (ma.getValue().contains(s)) {

                        allJobs.get(key).put(ma.getKey(), ma.getValue());

                        break;
                    }
                }
            }
        }

        //查看每一个类别有多少个岗位
        for (Map.Entry<String, HashMap<String, String>> m : allJobs.entrySet()) {

            logger.debug(m.getKey() + " " + m.getValue().size());
        }

        return allJobs;
    }
}
