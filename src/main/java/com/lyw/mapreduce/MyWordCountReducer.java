package com.lyw.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.Iterator;

public class MyWordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Text outKey;
    private IntWritable outValue;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values){
            sum += value.get();
        }
        outKey.set(key);
        outValue.set(sum);
        context.write(outKey, outValue);
    }
}
