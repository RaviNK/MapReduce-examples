/**
 * Created by ravi on 21/4/16.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import javax.swing.text.html.HTMLDocument.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Snprog1 {
    public static class TokenizerMapper
            extends Mapper<Object, Text,Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text num = new Text("number");
        private int count;
        private int[] no ;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //StringTokenizer itr = new StringTokenizer(value.toString());
            StringTokenizer newitr = new StringTokenizer(value.toString(),",");

            while (newitr.hasMoreTokens()) {
                word.set(newitr.nextToken());
                context.write(num,new IntWritable(Integer.parseInt(word.toString())));
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,Integer> {
        private IntWritable result = new IntWritable();


        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            // ArrayList<IntWritable> list = new ArrayList<IntWritable>();
        /* ArrayList<IntWritable> even = new ArrayList<IntWritable>();
            ArrayList<IntWritable> odd = new ArrayList<IntWritable>();
          IntWritable outVal = new IntWritable();
          Text outKey = new Text("e1");
          Text outKey1 = new Text("odd");
          T
          if((val.get() % 2 == 0) && cnt < 5){

                    outVal = val ;
                    context.write(outKey,outVal);

                }
                //list.add(val);

                else if ((val.get() % 2 == 1) && cnt < 5) {
                    outVal = val ;
                    context.write(outKey1,outVal);
                }
          int cnt = 0 ;*/
            Text no = new Text("Number");
            Text sq = new Text("Square");
            Text cube = new Text("Cube");
            for (IntWritable val : values) {
                IntWritable n  =  new IntWritable(val.get());
                int num = n.get();
                context.write(no, val.get());
                context.write(sq, (num*num));
                context.write(cube,num*num*num);


            }
            // context.write(key, new MyArrayWritable(IntWritable.class, list.toArray(new IntWritable[list.size()])));


        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "numbers_tb");
        job.setJarByClass(Snprog1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


