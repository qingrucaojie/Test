package mymr.day19;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

public class MutlipleChain extends Configured implements Tool{

	
	public static class MutlipleChainMap extends Mapper
		<LongWritable , Text , Text , Text>{
		Text k = new Text();
		Text v = new Text();
		@Override
		protected void map(LongWritable key, Text value, 
				Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			//标识不是标题行
			if(!line.contains("factoryname")){
				String[] splits = line.split("\t");
				if((splits[0].charAt(0)+"").matches("\\d{1}")){
					k.set(splits[0]);
					v.set("a" + splits[1]);
				}else{
					k.set(splits[1]);
					v.set("f" + splits[0]);
				}
				context.write(k, v);
			}
		}
	}
	
	public static class MutlipleChainReducer extends Reducer
		<Text , Text , Text , Text>{
		Text k = new Text();
		Text v = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, 
				Context context)
				throws IOException, InterruptedException {
			List<String> address = new ArrayList<>();
			List<String> factory = new ArrayList<>();
			for (Text value : values) {
				String line = value.toString();
				char c = line.charAt(0);
				String str = line.substring(1);
				if(c == 'a'){
					address.add(str);
				}else{
					factory.add(str);
				}
			}
			for (int i = 0; i < factory.size(); i++) {
				for (int j = 0; j < address.size(); j++) {
					k.set(factory.get(i));
					v.set(address.get(j));
					context.write(k, v);
				}
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
		job.setJobName("MutlipleChain");
		job.setJarByClass(MutlipleChain.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MutlipleChainMap.class);
		job.setReducerClass(MutlipleChainReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1 ;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MutlipleChain(), args));
	}


}
