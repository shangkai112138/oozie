package com.lxg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;

public class HDFSTest1 {


    private static String clusterName = "bigdata-om";
    private static String nn1Addr = "10.48.183.134:8020";
    private static String nn2Addr = "10.48.183.135:8020";

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String hdfsAddr =  "hdfs://" + clusterName;

        conf.set("fs.defaultFS", hdfsAddr);
        conf.set("dfs.nameservices", clusterName);
        conf.set("dfs.ha.namenodes." + clusterName, "nn1,nn2");
        conf.set("dfs.namenode.rpc-address." + clusterName+".nn1", nn1Addr);
        conf.set("dfs.namenode.rpc-address." + clusterName+".nn2", nn2Addr);
        conf.set("dfs.client.failover.proxy.provider." + clusterName,
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("dfs.permissions","false");

        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("dfgx_test001/bdoc@FJBDKDC","/root/dfgx_test001.keytab");

//        FileSystem fs = FileSystem.get(conf);
        FileSystem fs = FileSystem.get(URI.create(hdfsAddr), conf, "dfgx_test001/bdoc@FJBDKDC");


        Path path = new Path("/user/workflow/");

        try {
            if (!fs.exists(path)) {
                System.out.println("不存在該路徑+創建");
                fs.mkdirs(path);
                System.out.println("創建完成");
            }
        } catch (IOException e) {
            System.out.println("创建" + path.toString() + "失败");
            e.printStackTrace();
        }

        Path patha = new Path("/user/dfgx_test001/AI/workflow");

        try {
            if(!fs.exists(patha)){
                System.out.println("不存在該路徑");
                fs.mkdirs(patha);
                System.out.println("创建完成");
            }
        } catch (IOException e) {
            System.out.println("创建" + patha.toString() + "失败");
            e.printStackTrace();
        }

        try {
            fs.copyFromLocalFile(new Path("/root/workflow/workflow.xml"), patha);
            System.out.println("上传文件");
        } catch (IOException e) {
            System.out.println("111");
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            System.out.println("222");
            e.printStackTrace();
        }


        try {
            FileStatus[] fileStatuses = fs.listStatus(new Path("/user"));
            System.out.println("读取文件目录");
            if(fileStatuses != null && fileStatuses.length > 0){
                for (FileStatus fileStatus : fileStatuses) {
                    System.out.println(fileStatus.getPath().toString());
                }
            }
        } catch (IOException e) {
            System.out.println("333");
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            System.out.println("444");
            e.printStackTrace();
        }


    }
}
