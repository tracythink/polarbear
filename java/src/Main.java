import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.StringTokenizer;

public class Main {



    public static void main(String[] args) {
        System.out.println("Hello World!");

        final Text word = new Text();
        // final IntWritable one = new IntWritable(1);

        StringTokenizer itr = new StringTokenizer("THIS IS A TEST FILE.2423423 我的的  的刀锋TRre哥哥 。。。");
        while (itr.hasMoreTokens()) {
            System.out.println("EACH: "+itr.nextToken());

            //word.set(itr.nextToken());

            //context.write(this.word, one);
        }



    }
}
