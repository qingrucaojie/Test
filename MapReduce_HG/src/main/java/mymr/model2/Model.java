package mymr.model2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class Model extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Model(), args));
	}

	public static class ModelMap extends Mapper
			<LongWritable , Text , Text , Text>{
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
		}
	}
	
	public static class ModelReducer extends Reducer
			<Text , Text , Text , Text>{
		@Override
		protected void setup(Context context) 
				throws IOException, InterruptedException {
			
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, 
				Context context)
				throws IOException, InterruptedException {
			
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 2){
			System.out.println("usage ... <in> ... <out>");
			System.err.println("输入的参数不合法...");
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
		return job.waitForCompletion(true) ? 0 : 1;
	}
}












