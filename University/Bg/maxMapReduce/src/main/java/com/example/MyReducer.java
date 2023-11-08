package com.example;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    static LongWritable result = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Long max = -1L;
        for(LongWritable num : values){
            if(num.get() > max){
                max = num.get();
            }
        }

        result.set(max);
        context.write(new Text("max"), result);
    }
}