package com.itcast.clean;

import com.itcast.dao.MongoDBStorage;
import com.mongodb.MongoClient;

import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 清洗数据具体操作
 */
public class JobCleanUtils {

    /**
     * 清洗发布日期（将中文去除）
     * @param amount 清洗之前数据
     * @return 清洗之后数据
     */
    public static String cleanAmount(String amount) {

        Pattern pattern = Pattern.compile("[\\u4e00-\\u9fa5]");

        Matcher matcher = pattern.matcher(amount);

        return matcher.replaceAll("").trim();
    }

    /**
     * 清洗工作经验（以年为单位的数字）
     * @param experience 工作经验
     * @return 数字
     */
    public static int cleanExperience(String experience) {

        String message = "";

        if (experience == "") {

            return 0;
        } else {

            if (experience.contains("无")) {

                return 0;
            } else {

                if (experience.contains("-")) {

                    return Integer.parseInt(experience.substring(0, experience.indexOf("-")));
                } else {

                    return Integer.parseInt(cleanAmount(experience));
                }
            }
        }
    }

    /**
     * 清洗公司地点（扩充到具体的省份）
     * @param location 地点
     * @return 省份名称
     */
    public static String cleanLocation(String location) {

        //解析地点
        String loc;

        if (location.contains("-")) {

            loc = location.substring(0, location.indexOf("-"));

        } else {

            loc = location;
        }

        MongoDBStorage mongoDBStorage = MongoDBStorage.getInstance();

        //初始化连接
        MongoClient mongoClient = mongoDBStorage.init();

        String database = "job_internet";

        //找到父城市名称
        Object parent_code = mongoDBStorage.getParentCityCode(mongoClient, database, "China_areas_location", 2854, loc);

        Object parentCode = mongoDBStorage.getParentCityCode(mongoClient, database, "China_cities_location", 343, (String) parent_code);

        String locname = mongoDBStorage.getParentCityName(mongoClient, database, "China_province_location", 33, parentCode);

        //关闭连接
        mongoDBStorage.close();

        if (locname != null) {

            return locname.substring(0, locname.length() - 1);
        } else {

            return loc;
        }
    }

    /**
     * 清洗工资（将工资都换算成元/月，浮动的取中间）
     * @param salary 工资
     * @return 清洗后的工资
     */
    public static String cleanSalary(String salary) {

        DecimalFormat decimalFormat = new DecimalFormat("0.00");

        Double sal;

        //取工资的中间值
        if (salary.contains("-")) {

            Double first = Double.valueOf(salary.substring(0, salary.indexOf("-")));

            Double second = Double.valueOf(salary.substring(salary.indexOf("-") + 1, salary.indexOf("/") - 1));

            sal = (first + second) / 2;
        } else {

            if (salary == "") {

                sal = 0.0;
            } else {

                //将工资除了小数点和数据留下其余去掉
                Integer length = salary.length();

                String sal2 = "";

                for (int i = 0; i < length; i++) {

                    char c = salary.charAt(i);

                    if ((c >= 48 && c <=57) || c == 46) {

                        sal2 += c;
                    }
                }

                sal = Double.valueOf(sal2);
            }
        }

        //将工资换算成元/月
        if (salary.contains("万")) {

            sal = sal * 10000;
        } else if (salary.contains("千")) {

            sal = sal * 1000;
        } else if (salary.contains("年")) {

            sal = sal / 12;
        } else if (salary.contains("日") || salary.contains("天")) {

            sal = sal * 30;
        }

        return decimalFormat.format(sal);
    }

    /**
     * 清洗招聘人数（用区间表示）
     * @param scale 招聘人数
     * @return 招聘区间
     */
    public static String cleanScale(String scale) {

        String message = "";

        scale = scale.split("-")[0];

        if (scale.contains("少") && scale.contains("50")) {

            message = "(0,50)";
        } else if (scale.contains("10000")) {

            message = "(10000,+)";
        } else if (scale.contains("5000")) {

            message = "(5000,10000)";
        } else if (scale.contains("1000")) {

            message = "(1000,5000)";
        } else if (scale.contains("500")) {

            message = "(500,1000)";
        } else if (scale.contains("150")) {

            message = "(150,500)";
        } else if (scale.contains("50")) {

            message = "(50,150)";
        }

        return message;
    }
}
