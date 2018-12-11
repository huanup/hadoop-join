package com.mine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by zhanghuan on 2018/12/10.
 */
public interface Common {

    String HDFS = "hdfs://localhost:8020//";

    static String getPath(String filePath) {
        return HDFS +  filePath;
    }

    static void delete(String dirPath, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(getPath(dirPath));
        if (fs.exists(targetPath)) {
            boolean delResult = fs.delete(targetPath, true);
            if (delResult) {
                System.out.println(targetPath + " has been deleted sucessfullly.");
            } else {
                System.out.println(targetPath + " deletion failed.");
            }
        }
    }
}