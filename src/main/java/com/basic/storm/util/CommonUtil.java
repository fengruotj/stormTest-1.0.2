package com.basic.storm.util;

import java.io.File;

/**
 * Created by 79875 on 2017/2/18.
 */
public class CommonUtil {
    public static final String WORD_COUNT_PATH="wordcountPath";
    public static final String OUTPUT_RESULT_PATH="outputresultPath";
    public static final String SPOUT_COUNT_PATH="spoutcountPath";
    private static final String TUPLE_COUNT_PATH = "tuplecountPath";

    public static void deleteFiles(){

        //删除记录的文件
        File wordcountTxt=new File(PropertiesUtil.getProperties(WORD_COUNT_PATH));
        File tuplecountTxt=new File(PropertiesUtil.getProperties(TUPLE_COUNT_PATH));
        File outputresultTxt=new File(PropertiesUtil.getProperties(OUTPUT_RESULT_PATH));
        File spoutcountTxt=new File(PropertiesUtil.getProperties(SPOUT_COUNT_PATH));
        if(wordcountTxt.exists()){
            wordcountTxt.delete();
        }
        if(tuplecountTxt.exists()){
            tuplecountTxt.delete();
        }
        if(outputresultTxt.exists()){
            outputresultTxt.delete();
        }
        if(spoutcountTxt.exists()){
            spoutcountTxt.delete();
        }
    }
}
