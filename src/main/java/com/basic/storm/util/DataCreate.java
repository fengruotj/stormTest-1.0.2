package com.basic.storm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by 79875 on 2017/1/13.
 */
public class DataCreate {
    public static final String outPutFile="D://bigdata-word-datasource.txt";
    public static Logger logger= LoggerFactory.getLogger(DataCreate.class);

    public static boolean controllGenerate=true;

    //句子
    public static String[] sentences={
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "dont have acow man",
            "i dont think i like fleas",
            "Happiness is a way station between too much and too little",
            "The hard part isn’t making the decision. It’s living with it",
            "Your happy passer-by all knows, my distressed there is no place hides",
    };

    public static void runCreateDataSource() throws Exception {

        File output = new File(outPutFile);
//        if(output.exists()){
//            output.delete();
//        }
//
        int count=0;
        Random random=new Random();

        //生成单词词库
        List<String> words=new ArrayList<String>();
        for(int i=0;i<sentences.length;i++){
            for(String word:sentences[i].split(" ")){
                words.add(word);
            }
        }

        while (controllGenerate){
            int ran=random.nextInt(words.size());
            FileUtil.writeAppendTxtFile(output,words.get(ran)+" ");
            count++;

            //写入换行
            if(count==15){
                FileUtil.writeAppendTxtFile(output,"\r\n");
                count=0;
            }
            logger.info("插入一条文本记录："+words.get(ran));
        }
    }

    public static void stopGenertate(){
        controllGenerate=false;
    }
}
