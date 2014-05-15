package com.orachestrall.tracy.mt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: Schiffer.huang
 * Date: 14-5-14
 * Time: ÏÂÎç4:05
 * LogPlayer
 */
public final class LogPlayer {


    public static class LogMapper  extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text hourWord = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            SimpleDateFormat formatter2 = new SimpleDateFormat("yy-MM-dd");
            Date d1 =new Date();
            d1.setTime(System.currentTimeMillis()-1*24*3600*1000);

            String strDate =formatter2.format(d1);
            if(line.contains(strDate)){
                String[] strArr = line.split(",");
                int len = strArr[0].length();
                String time = strArr[0].substring(1,len-1);

                String[] timeArr = time.split(":");
                String strHour = timeArr[0];
                String hour = strHour.substring(strHour.length()-2,strHour.length());
                String hourKey = "";

                if(line.contains("Exception")){
                    word.set("Exception");
                    context.write(word, one);
                    hourKey = "Exception:" + hour;
                    hourWord.set(hourKey);
                    context.write(hourWord, one);
                    word.clear();
                    hourWord.clear();
                }

                if(line.contains("SocketException")){
                    word.set("SocketExceptionCount");
                    context.write(word, one);
                    hourKey = "SocketExceptionCount:" + hour;
                    hourWord.set(hourKey);
                    context.write(hourWord, one);
                    word.clear();
                    hourWord.clear();
                }
            }
        }
    }


    public static class LogReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static int run(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = new Job(conf, "logplayer");

        job.setJarByClass(LogPlayer.class);
        job.setMapperClass(LogMapper.class);
        job.setCombinerClass(LogReducer.class);
        job.setReducerClass(LogReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Date startTime = new Date();
        System.out.println("TRACY gogogogo----------- start time : " + startTime);
        int ret = job.waitForCompletion(true)? 0 : 1;


        Date end_time = new Date();
        System.out.println("TRACY ended:  end time " + end_time);
        System.out.println("TRACY job took " + (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
        return ret;
    }


    public static void main(String[] args) {
        try {int ret = run(args); System.exit(ret);} catch (Exception e) { e.printStackTrace(); System.out.println(e.getMessage());}
    }


}
