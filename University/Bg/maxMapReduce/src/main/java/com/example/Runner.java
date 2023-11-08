package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Runner {
    public static final String INPUT = "input3.txt";
    public static final String OUTPUT = "output";

    public static void main(String[] args) throws Exception {
        org.apache.log4j.BasicConfigurator.configure();
        Runner.deleteOutput(new File(Runner.OUTPUT));
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MaxInList");
        job.setJarByClass(Runner.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(Runner.INPUT));
        FileOutputFormat.setOutputPath(job, new Path(Runner.OUTPUT));
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