package mymr.day20;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class UseMyPartition extends Configured implements Tool{

	
	public static class UseMyPartitionMap extends Mapper
		<LongWritable , Text , IntWritable , NullWritable>{
		IntWritable k = new IntWritable();
		@Override
		protected void map(LongWritable key, Text value, 
				Context context)
				throws IOException, InterruptedException {
			k.set(Integer.parseInt(value.toString()));
			context.write(k , NullWritable.get());
		}
	}
	
	public static class UseMyPartitionReducer extends Reducer
		<IntWritable , NullWritable , IntWritable , NullWritable>{
		@Override
		protected void reduce(IntWritable key, Iterable<NullWritable> values, 
				Context context)
				throws IOException, InterruptedException {
			context.write(key , NullWritable.get());
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 2){
			System.out.println("usage ... <in>...<out>");
			System.err.println("ƒ„µƒ ‰»Î”–ŒÛ...");
			System.exit(2);
		}
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("UseMyPartition");
		job.setJarByClass(UseMyPartition.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(UseMyPartitionMap.class);
		job.setReducerClass(UseMyPartitionReducer.class);
		job.setNumReduceTasks(3);
		job.setPartitionerClass(MyPartition.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1 ;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new UseMyPartition(), args));
	}


}
