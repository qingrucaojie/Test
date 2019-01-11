package mymr.day17;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//获取文件中数字的最大值
public class GetMaxNum extends Configured implements Tool{

	public static class GetMaxNumMap extends Mapper
		<LongWritable , Text , NullWritable , Text>{
		@Override
		protected void map(LongWritable key, Text value, 
				Context context)
				throws IOException, InterruptedException {
			//数据去到reducer之后会在同一个集合中   可以比较
			context.write(NullWritable.get() , value);
		}
	}
	
	public static class GetMaxNumReducer extends Reducer
		< NullWritable , Text , Text , Text>{
		@Override
		protected void reduce(NullWritable key, Iterable<Text> values, 
				Context context)
				throws IOException, InterruptedException {
			int maxNum = Integer.MIN_VALUE;
			for (Text value : values) {
				int current = Integer.parseInt(value.toString());
				if(current > maxNum){
					maxNum = current;
				}
			}
			context.write(new Text("当前值中的最大值是 : "), new Text(maxNum+""));
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
		job.setJobName("GetMaxNum");
		job.setJarByClass(GetMaxNum.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(GetMaxNumMap.class);
		job.setReducerClass(GetMaxNumReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1 ;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new GetMaxNum(), args));
	}


}
