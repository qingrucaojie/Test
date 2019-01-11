package mymr.day20;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SingleChain extends Configured implements Tool{

	
	public static class SingleChainMap extends Mapper
		<LongWritable , Text , Text , Text>{
		Text k = new Text();
		Text v = new Text();
		@Override
		protected void map(LongWritable key, Text value, 
				Context context)
				throws IOException, InterruptedException {
			 String line = value.toString();
			 if(!line.contains("parent")){
				 String[] splits = line.split("\t");
				 k.set(splits[0]);
				 v.set("p" + splits[1]);
				 context.write(k, v);
				 
				 k.set(splits[1]);
				 v.set("c" + splits[0]);
				 context.write(k, v);
			 }
		}
	}
	
	public static class SingleChainReducer extends Reducer
		<Text , Text , Text , Text>{
		Text k = new Text();
		Text v = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, 
				Context context)
				throws IOException, InterruptedException {
			List<String> grandParent = new ArrayList<>();
			List<String> grandChild = new ArrayList<>();
			
			for (Text value : values) {
				String line = value.toString();
				char c = line.charAt(0);
				String str = line.substring(1);
				if(c == 'p'){
					grandParent.add(str);
				}else{
					grandChild.add(str);
				}
			}
			for (int i = 0; i < grandChild.size(); i++) {
				for (int j = 0; j < grandParent.size(); j++) {
					k.set(grandChild.get(i));
					v.set(grandParent.get(j));
					context.write(k, v);
				}
			}
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
		job.setJobName("SingleChain");
		job.setJarByClass(SingleChain.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(SingleChainMap.class);
		job.setReducerClass(SingleChainReducer.class);
//		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1 ;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new SingleChain(), args));
	}


}
