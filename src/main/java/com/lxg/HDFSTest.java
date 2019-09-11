package com.lxg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
public class HDFSTest {

    public static void main(String[] args) throws Exception {
        String flag = args[0];
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("dfgx_test001/bdoc@FJBDKDC","/root/dfgx_test001.keytab");

        FileSystem fs = FileSystem.get(conf);

        if(flag.trim().equals("1")){
            System.out.println("11111111");
            fs.copyFromLocalFile(new Path("/root/123.csv"),new Path("/user/lxg_test.csv"));
        }

        System.out.println("222222222");

        try {
            System.out.println(fs.exists(new Path("/user/lxg_test.csv")));
        } catch (Exception e) {
            System.out.println("判断是否存在该文件");
            e.printStackTrace();
        }

        try {
            fs.listStatus(new Path("/user/lxg_test.csv"));
            FileStatus fileStatus = fs.getFileStatus(new Path("/user/lxg_test.csv"));
            System.out.println("sssss"+fileStatus.getPath().toString());
            System.out.println("文件大小=="+fileStatus.getLen());
            System.out.println();


        } catch (Exception e) {
            System.out.println("判断是否存在该文件");
            e.printStackTrace();
        }

        fs.copyToLocalFile(new Path("/user/lxg_test.csv"),new Path("/root/1234.csv"));

    }
}
