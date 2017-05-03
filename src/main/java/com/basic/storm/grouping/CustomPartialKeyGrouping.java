package com.basic.storm.grouping;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;

/**
 * locate com.basic.storm.grouping
 * Created by 79875 on 2017/5/3.
 */
public class CustomPartialKeyGrouping implements CustomStreamGrouping{
    private List<Integer> targetTasks;
    private  long[] targetTaskStats;
    WorkerTopologyContext context;
    //private HashFunction h1 = Hashing.murmur3_128(13);
    //private HashFunction h2 = Hashing.murmur3_128(17);

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
                        List<Integer> targetTasks) {
        // TODO Auto-generated method stub
        this.context = context;
        this.targetTasks = targetTasks;
        targetTaskStats = new long[this.targetTasks.size()];
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        // TODO Auto-generated method stub

        List<Integer> boltIds = new ArrayList();
        if(values.size()>0)
        {
            String str = values.get(0).toString();
            if(str.isEmpty())
                boltIds.add(targetTasks.get(0));
            else
            {
                //int firstChoice = (int) (FastMath.abs(h1.hashBytes(str.getBytes()).asLong()) % this.targetTasks.size());
                //int secondChoice = (int) (FastMath.abs(h2.hashBytes(str.getBytes()).asLong()) % this.targetTasks.size());
                int selected = getMinIndex(targetTaskStats);
                boltIds.add(targetTasks.get(selected));
                targetTaskStats[selected]++;
            }

        }

        return boltIds;
    }

    public int getMinIndex(long[] args){
        int minIndex = 0;
        //设定一个数，用于存放最小值
        long min=args[0];
        for(int i=1;i<args.length;i++){
            //依次将max与数组中的每个元素比较，将较小值赋予min
            if(min>args[i]){
                min = args[i];
                minIndex=i;
            }

        }
        //返回获得最小值
        return minIndex;
    }

}
