package mymr.day18;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCountDefineType extends Configured implements Tool{

	
	public static class WordCountCombinerMap extends Mapper
		<LongWritable , Text , Text , MyIntWritable>{//后两个参数因业务不明Text占位
		@Override
		protected void map(LongWritable key, Text value, 
				Context context)
				throws IOException, InterruptedException {
			String[] splits = value.toString().split("\\s+");
			for (String str : splits) {
				context.write(new Text(str), new MyIntWritable(1));
			}
		}
	}
	//自定义combiner
	/*public static class WordCountCombinerCombiner extends 
			Reducer<Text , IntWritable , Text , IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			context.write(key, new IntWritable(count));
		}
	}*/
	
	public static class WordCountCombinerReducer extends Reducer
		<Text , MyIntWritable , Text , MyIntWritable>{
		@Override
		protected void reduce(Text key, Iterable<MyIntWritable> values, 
				Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (MyIntWritable value : values) {
				count += value.get();
			}
			context.write(key, new MyIntWritable(count));
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
		job.setJobName("WordCountDefineType");
		job.setJarByClass(WordCountDefineType.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MyIntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MyIntWritable.class);
		job.setMapperClass(WordCountCombinerMap.class);
		//由于reducer中的方法就是我想要实现的功能,所以没必要重复的写代码,所以直接引入Ruducer类
		//由于业务逻辑的相同,代码才可以复用
		job.setCombinerClass(WordCountCombinerReducer.class);
//		job.setNumReduceTasks(0);
		job.setReducerClass(WordCountCombinerReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1 ;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCountDefineType(), args));
	}


}
