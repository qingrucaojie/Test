package mymr.day17;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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

public class DuplicateRemove {

	public static class DuplicateRemoveMap extends 
		Mapper<LongWritable , Text , Text , NullWritable>{
		@Override
		protected void map(LongWritable key, Text value, 
				Context context)
				throws IOException, InterruptedException {
			// \\s :ƥ��հ��ַ�  + :һ�λ���
			String[] splits = value.toString().split("\\s+");
			for (String str : splits) {
				context.write(new Text(str), NullWritable.get());
			}
		}
	}
	//===================������Ĭ�ϵĺϲ� ,key(����)��Ψһ��================
	public static class DuplicateRemoveReduce extends 
		Reducer<Text , NullWritable , Text , NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			
			context.write(key, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length < 2){
			System.out.println("usage ... <in> ... <out>");
			System.err.println("����Ĳ������Ϸ�...");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.map.output.compress.codec", 
				"org.apache.hadoop.io.compress.DefaultCodec");
		Job job = Job.getInstance(conf);
		job.setJobName("DuplicateRemove");
		job.setJarByClass(DuplicateRemove.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(DuplicateRemoveMap.class);
		job.setReducerClass(DuplicateRemoveReduce.class);
//		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}







