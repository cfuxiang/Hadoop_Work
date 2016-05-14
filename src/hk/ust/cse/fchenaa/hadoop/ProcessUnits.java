package hk.ust.cse.fchenaa.hadoop;

import java.util.*; 

import java.io.IOException; 
import java.io.IOException; 

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*; 

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ProcessUnits { 

	public static class TokenizerMapper
		extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer
		extends Reducer<Text,IntWritable,Text,IntWritable> {
		
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}	

   /*
   //Mapper class 
   public static class E_EMapper extends MapReduceBase implements 
	   Mapper<LongWritable ,//Input key Type 
	   Text,                //Input value Type
	   Text,                //Output key Type
	   IntWritable>        //Output value Type
	   { 

        //Map function 
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { 
           String line = value.toString();
           if(line != null) 
        	  line = line.trim();
           String lasttoken = null; 
           StringTokenizer s = new StringTokenizer(line,"\t"); 
           String year = s.nextToken();
           //check for null in year
           if(year != null)
        	   year = year.trim();

           while(s.hasMoreTokens()) {
               lasttoken=s.nextToken();
           } 

           //check for null 
           if(lasttoken != null) {
        	   lasttoken = lasttoken.trim();
        	   int avgprice = Integer.parseInt(lasttoken); 
	           output.collect(new Text(year), new IntWritable(avgprice)); 
	           System.out.println(year + " " + avgprice);
           }
        } 
     } 
   
     //Reducer class 
     public static class E_EReduce extends MapReduceBase implements 
   	  Reducer< Text, IntWritable, Text, IntWritable > {  

        //Reduce function 
        public void reduce( Text key, Iterator <IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { 
            int maxavg=30; 
            int val=Integer.MIN_VALUE; 

            output.collect(key, new IntWritable(val)); 

            while (values.hasNext()) { 
               if((val=values.next().get())>maxavg) { 
                  output.collect(key, new IntWritable(val)); 
               } 
            } 
         } 
    }  
	*/

	//Main function 
	public static void main(String args[]) throws Exception { 
		/*
        JobConf conf = new JobConf(ProcessUnits.class); 

        conf.setJobName("max_eletricityunits"); 
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class); 
        conf.setMapperClass(E_EMapper.class); 
        conf.setCombinerClass(E_EReduce.class); 
        conf.setReducerClass(E_EReduce.class); 
        conf.setInputFormat(TextInputFormat.class); 
        conf.setOutputFormat(TextOutputFormat.class); 

        FileInputFormat.setInputPaths(conf, new Path(args[0])); 
        FileOutputFormat.setOutputPath(conf, new Path(args[1])); 

        JobClient.runJob(conf); 
		 */
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(ProcessUnits.class);
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