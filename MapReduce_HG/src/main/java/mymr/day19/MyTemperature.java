package mymr.day19;

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

public class MyTemperature extends Configured implements Tool{

	
	public static class MyTemperatureMap extends Mapper
		<LongWritable , Text , Text , IntWritable>{//后两个参数因业务不明Text占位
		private static int MESSING = 9999;
		Text k = new Text();
		IntWritable v = new IntWritable();
		@Override
		protected void map(LongWritable key, Text value, 
				Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String year = line.substring(15,19);
			int airTemperature = 0;
			if(line.charAt(87) == '+'){
				airTemperature = Integer.parseInt(line.substring(88,92));
			}else{
				airTemperature = Integer.parseInt(line.substring(87,92));
			}
			
			String quality = line.substring(92,93);
			if(airTemperature != MESSING && quality.matches("[01459]")){
				 k.set(year);
				 v.set(airTemperature);
				context.write(k , v);
			}
		}
	}
	
	public static class MyTemperatureReducer extends Reducer
		<Text , IntWritable , Text , IntWritable>{
		Text k = new Text();
		IntWritable v = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, 
				Context context)
				throws IOException, InterruptedException {
			int maxTemperature = Integer.MIN_VALUE;
			int minTemperature = Integer.MAX_VALUE;
			
			int sumTemp = 0;
			int index = 0;
			for (IntWritable value  : values) {
				int currentTemp = value.get();
				if(currentTemp > maxTemperature){
					maxTemperature = currentTemp;
				}
				if(currentTemp < minTemperature){
					minTemperature = currentTemp;
				}
				sumTemp += currentTemp;
				index++;
			}
			String year = key.toString();
			k.set(year + "最高温度 : ");
			v.set(maxTemperature);
			context.write(k , v);
			k.set(year + "最低温度 : ");
			v.set(minTemperature);
			context.write(k , v);
			if(index != 0){
				k.set(year + "平均温度 : ");
				v.set(sumTemp / index);
				context.write(k , v);
			}
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
		job.setJobName("MyTemperature");
		job.setJarByClass(MyTemperature.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(MyTemperatureMap.class);
		job.setReducerClass(MyTemperatureReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1 ;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MyTemperature(), args));
	}


}
