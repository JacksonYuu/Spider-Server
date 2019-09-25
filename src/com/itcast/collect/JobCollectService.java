package com.itcast.collect;

import com.itcast.dao.HBaseStorage;
import com.itcast.dao.JobDataRepository;
import com.itcast.tools.ReadFile;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 爬取所有job的信息
 */
public class JobCollectService {

    //日志打印类
    private Logger logger = LoggerFactory.getLogger(getClass());

    //配置文件路径
    String path = System.getProperty("user.dir") + "/configuration/job_config.json";

    //读取配置文件信息
    String file = ReadFile.readFile(path);

    JSONObject jsonObject = new JSONObject(file);

    //初始化URL
    private String initUrl = jsonObject.getString("initUrl");

    //信息来源网站名称
    String wbsitename = jsonObject.getString("wbsitename");

    //保存岗位地址
    private List<String> allJobUrls = new ArrayList<>();

    //保存岗位信息地址
    private List<String> allJobMessageUrls = new ArrayList<>();

    private String url = "http://localhost:8080";

    /**
     * 构造方法
     */
    public static JobCollectService instance;

    public static synchronized JobCollectService getInstance() {
        if (instance == null) {
            instance = new JobCollectService();
        }
        return instance;
    }

    /**
     * 得到所有岗位地址
     */
    public void getAllJobUrls() {
        try {

            //添加初始化链接
            allJobUrls.add(initUrl);

            for (int i = 0; i < allJobUrls.size(); i++) {

                //连接链接并得到链接内容
                Document document = Jsoup.connect(url + allJobUrls.get(i)).get();

                //根据DOM得到岗位地址
                Elements elements = document.getElementById("pagination").getElementsByTag("a");

                for (Element element : elements) {

                    String href = element.attr("href");

                    if (!allJobUrls.contains(href)) {

                        logger.debug(href);

                        allJobUrls.add(href);
                    }
                }
            }
        } catch (IOException e) {

            logger.debug("There is a problem here!");

            logger.debug(e.toString());
        }
    }

    /**
     * 得到所有岗位信息地址
     */
    public void getAllJobMessageUrls() {
        try {

            for (int i = 0; i < allJobUrls.size(); i++) {

                //连接链接并得到链接内容
                Document document = Jsoup.connect(url + allJobUrls.get(i)).get();

                //根据DOM得到岗位信息地址
                Elements elements = document.getElementsByClass("PositionName");

                for (Element element : elements) {

                    Elements a = element.getElementsByTag("a");

                    String href = a.get(0).attr("href");

                    logger.debug(href);

                    allJobMessageUrls.add(href);
                }
            }

        } catch (IOException e) {

            logger.debug("There is a problem here!");

            logger.debug(e.toString());
        }
    }

    /**
     * 解析配置文件
     * @return 每个列和对应的配置文件信息
     */
    public HashMap<String, String[]> analyseConfiguration() {

        HashMap<String, String[]> map = new HashMap<>();

        JSONArray fields = jsonObject.getJSONArray("fields");

        Integer length = fields.length();

        for (int i = 0; i < length; i++) {

            JSONObject field = fields.getJSONObject(i);

            String[] message = new String[field.length()];

            String name = field.getString("name");

            message[0] = name;

            String chinesename = field.getString("chinesename");

            message[1] = chinesename;

            String type = field.getString("type");

            message[2] = type;

            String value = field.getString("value");

            message[3] = value;

            String position = field.getString("position");

            message[4] = position;

            String text = field.getString("text");

            message[5] = text;

            map.put(name, message);
        }

        return map;
    }

    /**
     * 得到所有岗位信息
     * @param tablename 数据表名称
     * @param family 列族名称
     */
    public void getJobMessage(String tablename, String family) {
        try {

            getAllJobUrls();

            getAllJobMessageUrls();

            //保存岗位信息
            HashMap<String, Object> allJobMessage = new HashMap<>();

            //得到配置文件的信息
            HashMap<String, String[]> map = analyseConfiguration();

            Integer size = allJobMessageUrls.size();

            for (int i = 0; i < size; i++) {

                String falUrl = url + allJobMessageUrls.get(i);

                String pageID;

                Pattern pattern = Pattern.compile("/([0-9]+)\\.html");

                Matcher matcher = pattern.matcher(falUrl);

                if (matcher.find()) {
                    pageID = matcher.group(1);
                } else {
                    return;
                }

                allJobMessage.put("id", pageID);

                Document document = Jsoup.connect(falUrl).timeout(5000).get();

                for (Map.Entry<String, String[]> m : map.entrySet()) {

                    String name = m.getKey();

                    String[] message = m.getValue();

                    String chinesename = message[1];

                    String type = message[2];

                    String[] v = message[3].split(",");

                    String value = "";

                    for (String s : v) {

                        if (type.equals("class")) {

                            value = value + "." + s;
                        } else {

                            value = value + "#" + s;
                        }
                    }

                    Integer position = Integer.parseInt(message[4]);

                    String text = message[5];

                    Elements elements = document.select(value);

                    Integer fp = elements.get(position).getElementsByClass("fp").size();

                    if (elements.size() == 0) {

                        allJobMessage.put(name, "");

                        logger.debug(name + " -- This information does not exist!");
                    } else {

                        if (name.equals("jobmess")) {

                            //解析岗位信息得到岗位名称，岗位地点，岗位工资
                            JobCollectUtils.parseJobmess(elements, position, allJobMessage);

                            continue;
                        }

                        if (name.equals("requirement")) {

                            //解析公司要求得到工作经验，工作学历，招聘人数，发布时间
                            JobCollectUtils.parseRequirement(elements, position, allJobMessage);

                            continue;
                        }

                        if (name.equals("companymess")) {

                            //解析公司信息得到公司的特征，公司级别，公司产业
                            JobCollectUtils.parseCompanymess(elements.text(), allJobMessage);

                            continue;
                        }

                        if (name.equals("categoryAndKey")) {

                            Element ele = elements.get(position);

                            JobCollectUtils.parseKeyAndCategory(ele, fp, allJobMessage);

                            continue;
                        }

                        String t;

                        if (text.equals("text")) {

                            t = elements.get(position).text();

                            if (name.equals("description")) {

                                JobCollectUtils.splitColon(t, allJobMessage);
                            } else {

                                allJobMessage.put(name, t);
                            }
                        } else {

                            t = elements.get(position).attr(text);

                            allJobMessage.put(name, t);
                        }
                    }
                }

                for (Map.Entry a : allJobMessage.entrySet()) {

                    logger.debug(a.getKey() + "--" + a.getValue());
                }

                //初始化连接到HBase
                HBaseStorage hBaseStorage = HBaseStorage.getInstance();

                hBaseStorage.init();

                JobDataRepository jobDataRepository = JobDataRepository.getInstance();

                jobDataRepository.insertDataToHBase(hBaseStorage, tablename, family, allJobMessage);

                //关闭连接
                hBaseStorage.close();
            }
        } catch (IOException e) {

            logger.debug(e.toString());
        }
    }
}
