package com.lxg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
public class SparkSessionTest {


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

//
//        if (sparkSession == null) {
//
//
//
//
//
//            SparkContext sc = sparkSession.sparkContext();
//            sc.hadoopConfiguration().set("fs.defaultFS", "hdfs://" + clusterName);
//            sc.hadoopConfiguration().set("dfs.nameservices", clusterName);
//            sc.hadoopConfiguration().set("dfs.ha.namenodes." + clusterName, "nn1,nn2");
//            sc.hadoopConfiguration().set("dfs.namenode.rpc-address." + clusterName + ".nn1", nn1Addr);
//            sc.hadoopConfiguration().set("dfs.namenode.rpc-address." + clusterName + ".nn2", nn2Addr);
//            sc.hadoopConfiguration().set("dfs.client.failover.proxy.provider." + clusterName, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//
//        }





    }
}
