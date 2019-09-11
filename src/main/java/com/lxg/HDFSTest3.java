package com.lxg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class HDFSTest3 {

    public static void main(String[] args) throws Exception {
        String username = args[0];
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(username,"/root/dfgx_test001.keytab");
        FileSystem fs = FileSystem.get(conf);

        try {
            System.out.println("下载文件");
            fs.copyToLocalFile(new Path("/user/dfgx_test001/1.txt"),new Path("/root/1.txt"));
            System.out.println("下载成功");
        } catch (IOException e) {
            System.out.println("下载失败1");
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            System.out.println("下载失败");
            e.printStackTrace();
        }




    }
}
