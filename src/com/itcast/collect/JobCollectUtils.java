package com.itcast.collect;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.HashMap;

/**
 * 关于对于岗位信息处理的工具
 */
public class JobCollectUtils {

    /**
     * 解析岗位信息
     * @param elements 岗位信息
     * @param position 位置信息
     * @return 解析完成的岗位信息
     */
    public static HashMap<String, Object> parseJobmess(Elements elements, Integer position, HashMap<String, Object> allJobMessage) {

        Element element = elements.get(position);

        String jobName = element.getElementsByTag("h1").text();

        String location = element.getElementsByClass("lname").text();

        String salary = element.getElementsByTag("strong").text();

        allJobMessage.put("jobName", jobName.trim());

        allJobMessage.put("location", location.trim());

        allJobMessage.put("salary", salary.trim());

        return allJobMessage;
    }

    /**
     * 将信息以冒号切割
     * @param text 需要切割的字符串
     * @return 切割完成后的字符串
     */
    public static HashMap<String, Object> splitColon(String text, HashMap<String, Object> allJobMessage) {

        if (text.contains(":")) {

            String[] message = text.split(":");

            allJobMessage.put("description", message[message.length - 1].split("职能类别：")[0].trim());
        }

        return allJobMessage;
    }

    /**
     * 解析关键字和职业类型
     * @param text 需要解析的字符串
     * @param fp fp这个class的个数
     * @param allJobMessage 所有的岗位信息
     * @return 所有的岗位信息
     */
    public static HashMap<String, Object> parseKeyAndCategory(Element element, Integer fp, HashMap<String, Object> allJobMessage) {

        String text = element.text();

        if (fp == 2) {

            String text1 = element.getElementsByClass("fp").get(0).text();

            String[] message = text.split("：");

            String[] message1 = text1.split("：");

            allJobMessage.put("keywords", message[message.length - 1].trim());

            allJobMessage.put("category", message1[1].trim());
        } else if (fp == 1) {

            String text2 = element.getElementsByClass("fp").get(0).text();

            String[] message = text2.split("：");

            if (text.contains("职能类别")) {

                allJobMessage.put("keywords", "");

                allJobMessage.put("category", message[message.length - 1].trim());
            } else {

                allJobMessage.put("keywords", message[message.length - 1].trim());

                allJobMessage.put("category", "");
            }
        } else {

            allJobMessage.put("keywords", "");

            allJobMessage.put("category", "");
        }

        return allJobMessage;
    }

    /**
     * 解析公司要求
     * @param elements 需要解析的信息
     * @param position 位置信息
     * @return 解析完成后的字符串
     */
    public static HashMap<String, Object> parseRequirement(Elements elements, Integer position, HashMap<String, Object> allJobMessage) {

        HashMap<String, String> map = new HashMap<>();

        Elements elements1 = elements.get(position).getElementsByClass("sp4");

        String[] message = new String[4];

        for (int i = 0; i < message.length; i++) {

            for (int j = 0; j < elements1.size(); j++) {

                if (elements1.get(j).getElementsByClass("i" + (i + 1)).size() != 0) {

                    message[i] = elements1.get(j).text();
                }
            }

            if (message[i] == null) {

                message[i] = "";
            }
        }

        allJobMessage.put("experience", message[0].trim());

        allJobMessage.put("education", message[1].trim());

        allJobMessage.put("amount", message[2].trim());

        allJobMessage.put("date", message[3].trim());

        return allJobMessage;
    }

    /**
     * 解析公司信息
     * @param text 公司信息
     * @param allJobMessage 所有的岗位信息
     * @return 所有的岗位信息
     */
    public static HashMap<String, Object> parseCompanymess(String text, HashMap<String, Object> allJobMessage) {

        String[] all = new String[3];

        String[] message = text.split("\\|");

        for (int i = 0; i < message.length; i++) {
            all[i] = message[i];
        }

        for (int i = 0; i < all.length; i++) {
            if (all[i] == null) {
                all[i] = "";
            }
        }

        allJobMessage.put("nature", all[0].trim());

        allJobMessage.put("scale", all[1].trim());

        allJobMessage.put("industry", all[2].trim());

        return allJobMessage;
    }
}
