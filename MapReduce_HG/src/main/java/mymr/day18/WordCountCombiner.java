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


public class WordCountCombiner extends Configured implements Tool{

	
	public static class WordCountCombinerMap extends Mapper
		<LongWritable , Text , Text , IntWritable>{//������������ҵ����Textռλ
		@Override
		protected void map(LongWritable key, Text value, 
				Context context)
				throws IOException, InterruptedException {
			String[] splits = value.toString().split("\\s+");
			for (String str : splits) {
				context.write(new Text(str), new IntWritable(1));
			}
		}
	}
	//�Զ���combiner
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
		<Text , IntWritable , Text , IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, 
				Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 2){
			System.out.println("usage ... <in>...<out>");
			System.err.println("�����������...");
			System.exit(2);
		}
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("WordCountCombiner");
		job.setJarByClass(WordCountCombiner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(WordCountCombinerMap.class);
		//����reducer�еķ�����������Ҫʵ�ֵĹ���,����û��Ҫ�ظ���д����,����ֱ������Ruducer��
		//����ҵ���߼�����ͬ,����ſ��Ը���
		job.setCombinerClass(WordCountCombinerReducer.class);
//		job.setNumReduceTasks(0);
		job.setReducerClass(WordCountCombinerReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1 ;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCountCombiner(), args));
	}


}
