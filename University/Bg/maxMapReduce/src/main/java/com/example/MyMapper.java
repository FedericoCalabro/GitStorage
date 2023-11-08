package com.example;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, LongWritable> {

    final static Text keyCostant = new Text("key");

    @Override
    public void run(Mapper<Object, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        long localMax = 0L;
        // c'era una volta
        // setup(context);
        try {
          while (context.nextKeyValue()) {
            // c'era una volta
            // map(context.getCurrentKey(), context.getCurrentValue(), context);
            Text value = context.getCurrentValue();
            Long num = Long.parseLong(value.toString());
            if(num > localMax) {
                localMax = num;
            }
          }
        } finally {
            // c'era una volta
            // cleanup(context);
            context.write(keyCostant, new LongWritable(localMax));
        }
    }
}