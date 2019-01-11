package mymr.day19;

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

/*
 * 二次排序
 * 使用的map()key的排序功能
 */
public class SecondSort extends Configured implements Tool{
	
	public static class SecondSortMap extends Mapper
		<LongWritable , Text , SecondSortKey , NullWritable>{//后两个参数因业务不明Text占位
		SecondSortKey k = new SecondSortKey();
		@Override
		protected void map(LongWritable key, Text value, 
				Context context)
				throws IOException, InterruptedException {
			String[] splits = value.toString().split("\\s+");
			int first = Integer.parseInt(splits[0]);
			int second = Integer.parseInt(splits[1]);
			k.set(first, second);
			context.write(k , NullWritable.get());
		}
	}
	
	public static class SecondSortReducer extends Reducer
		<SecondSortKey , NullWritable , SecondSortKey , NullWritable>{
		@Override
		protected void reduce(SecondSortKey key, Iterable<NullWritable> values, 
				Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
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
		job.setJobName("SecondSort");
		job.setJarByClass(SecondSort.class);
		job.setMapOutputKeyClass(SecondSortKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(SecondSortKey.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(SecondSortMap.class);
		job.setReducerClass(SecondSortReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1 ;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new SecondSort(), args));
	}


}
