package com.itcast.main;

import com.itcast.cluster.JobClusterService;

/**
 * 开始执行任务
 */
public class JobService {

    public static void main(String[] args) {

//        //爬取数据
//        JobCollectService jobCollectService = JobCollectService.getInstance();
//
//        jobCollectService.getJobMessage("job_all", "message");
//
//        //清洗数据
//        JobCleanService jobCleanService = JobCleanService.getInstance();
//
//        jobCleanService.cleanJobMessage("job_all", "job_cloud", "message", "cloud");

        //聚类数据
        JobClusterService jobClusterService = JobClusterService.getInstance();

        jobClusterService.clusterJobMessage(3);
    }
}
