package com.itcast.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 读取文件信息工具
 */
public class ReadFile {

    private static FileInputStream fileInputStream;

    private static InputStreamReader inputStreamReader;

    private static BufferedReader bufferedReader;

    /**
     * 根据路径读取文件信息
     * @param path 文件路径
     * @return 文件信息
     */
    public static String readFile(String path) {

        String allMessage = "";

        try {

            //创建文件输入流
            fileInputStream = new FileInputStream(path);

            //创建读取输入流并设置读取编码为UTF-8
            inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");

            //创建读取缓冲层
            bufferedReader = new BufferedReader(inputStreamReader);

            String message;

            //将读取的信息保存
            while ((message = bufferedReader.readLine()) != null) {
                allMessage += message;
            }
        } catch (Exception e) {
            System.out.println("There is a problem here!");
        } finally {
            //关闭各种连接
            if (bufferedReader != null) try {
                bufferedReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (inputStreamReader != null) try {
                inputStreamReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (fileInputStream != null) try {
                fileInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //将信息返回
        return allMessage;
    }
}
