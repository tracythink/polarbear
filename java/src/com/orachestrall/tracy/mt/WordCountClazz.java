package com.orachestrall.tracy.mt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Schiffer.huang
 * Date: 14-4-28
 * Time: ÏÂÎç3:51
 * To change this template use File | Settings | File Templates.
 */
public class WordCountClazz {


    public static void main(String [] p) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.print("Tracy is---------------------- " );
        System.out.print("Tracy is---------------------- " );
        final Configuration conf=new Configuration();
        Job job=new Job(conf,"word count1");
        job.setJarByClass(WordCountClazz.class);
        job.setMapperClass(TokenCounterMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(p[0]));
        FileOutputFormat.setOutputPath(job,new Path(p[1]));
        final Boolean wait=job.waitForCompletion(true);
        System.out.print("job.waitForCompletion =  "+ wait);
    }



}
