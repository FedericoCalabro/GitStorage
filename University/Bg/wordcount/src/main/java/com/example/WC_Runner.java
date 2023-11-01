package com.example;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WC_Runner {

    public static final String INPUT = "input.txt";
    public static final String OUTPUT = "output";

    public static void main(String[] args) throws Exception {
        org.apache.log4j.BasicConfigurator.configure();
        WC_Runner.deleteOutput(new File(WC_Runner.OUTPUT));
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(WC_Runner.class);
        job.setMapperClass(WC_Mapper.class);
        // job.setCombinerClass(WC_Reducer.class);
        job.setReducerClass(WC_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(WC_Runner.INPUT));
        FileOutputFormat.setOutputPath(job, new Path(WC_Runner.OUTPUT));
        job.waitForCompletion(true);
    }

    public static void deleteOutput(File folder) {
        File[] files = folder.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory())
                    deleteOutput(f);
                else
                    f.delete();
            }
        }
        folder.delete();
    }
}