package mymr.day20;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

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
/**
 * 人名展示
 * 把在黑名单中的人从人名显示中过滤掉
 * 使用优化资源的分布式缓存实现
 *
 */
public class ShowName extends Configured implements Tool{

	
	public static class ShowNameMap extends Mapper
		<LongWritable , Text , Text , NullWritable>{//后两个参数因业务不明Text占位
		Text k = new Text();
		@Override
		protected void map(LongWritable key, Text value, 
				Context context)
				throws IOException, InterruptedException {
			List<String> blackList = new ArrayList<>();
			File file = new File("nickname");//使用时只能使用别名
			if(file.getName() != null){
				BufferedReader br = 
						new BufferedReader(new FileReader(file));
				String str = null;
				while((str = br.readLine())!=null){
					blackList.add(str);
				}
			}
			String name = value.toString();
			if(!blackList.contains(name)){
				k.set(name);
				context.write(k , NullWritable.get());
			}
		}
	}
	
	public static class ShowNameReducer extends Reducer
		<Text , NullWritable , Text , NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, 
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
		job.setJobName("ShowName");
		job.setJarByClass(ShowName.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(ShowNameMap.class);
		job.setReducerClass(ShowNameReducer.class);
		//可以被所有几点公共访问的路径即可,会从这个节点上把文件加载到缓存中去
		//链接位  #xxx (别名)
		job.addCacheFile(new URI("hdfs://hadoop-11:9000/blackList#nickname"));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1 ;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ShowName(), args));
	}


}
