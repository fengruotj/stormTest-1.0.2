package com.basic.storm;

import com.basic.util.HdfsOperationUtil;
import org.junit.Test;

import java.io.IOException;

/**
 * locate com.basic.storm
 * Created by 79875 on 2017/6/1.
 */
public class HDFSSpoutTest {

    private HdfsOperationUtil hdfsOperationUtil=new HdfsOperationUtil();

    @Test
    public void unlockFileResultTweets() throws IOException {
        String lockDirPath="/user/root/wordcount/input/resultTweets";
        hdfsOperationUtil.deleteFile(lockDirPath+"/.lock");
        hdfsOperationUtil.renameFile(lockDirPath+"/resultTweets.txt.inprogress",lockDirPath+"/resultTweets.txt");
    }

    @Test
    public void unlockFile40Log() throws IOException {
        String lockDirPath="/user/root/wordcount/input/data_658038657_40";
        hdfsOperationUtil.deleteFile(lockDirPath+"/.lock");
        hdfsOperationUtil.renameFile(lockDirPath+"/data_658038657_40.log.inprogress",lockDirPath+"/data_658038657_40.log");
    }
}
