package mymr.model;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 常用数据类型
 * LongWritable ---> Long(long)
 * Text------------> String
 * IntWritable ----> int(Integer)
 * NullWritable----> null
 * ShortWritable---> short(Short)
 * DoubleWritable--> double(Double)
 * FloatWritable---> float(Float)
 * ByteWritable----> byte(Byte)
 * BooleanWritable-> boolean(Boolean)
 */
public class Model extends Configured implements Tool{

	
	public static class ModelMap extends Mapper
		<LongWritable , Text , Text , Text>{//后两个参数因业务不明Text占位
		@Override
		protected void map(LongWritable key, Text value, 
				Context context)
				throws IOException, InterruptedException {
		}
	}
	
	public static class ModelReducer extends Reducer
		<Text , Text , Text , Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, 
				Context context)
				throws IOException, InterruptedException {
			
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 2){
			System.out.println("usage ... <in>...<out>");
			System.err.println("你的输入有误...");
			System.exit(2);
		}
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("Model");
		job.setJarByClass(Model.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(ModelMap.class);
		job.setReducerClass(ModelReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1 ;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Model(), args));
	}


}
