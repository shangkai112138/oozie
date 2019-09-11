package com.lxg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class HDFSTest4 {


    public static void main(String[] args) throws Exception {

        String path = args[0];
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("dfgx_test001/bdoc@FJBDKDC","/root/dfgx_test001.keytab");
        FileSystem fs = FileSystem.get(conf);


        try {
            System.out.println("1111111");
            FileStatus[] fileStatuses = fs.listStatus(new Path(path));
            if(fileStatuses !=null && fileStatuses.length > 0){
                System.out.println("size==="+fileStatuses.length);
                for (FileStatus fileStatus : fileStatuses) {
                    System.out.println(fileStatus.getPath().toString());

                }
            }
            System.out.println("2222222");
        } catch (IOException e) {
            System.out.println("shibai1111");
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            System.out.println("shibai22222");
            e.printStackTrace();
        }


    }
}
