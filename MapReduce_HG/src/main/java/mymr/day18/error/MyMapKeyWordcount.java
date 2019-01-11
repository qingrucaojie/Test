package mymr.day18.error;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 词频统计的主类
 */
public class MyMapKeyWordcount {

	/*
	 * mapper类
	 * 把文件中的每一个按照空格分割的单词切除;每个单词标 1 
	 */
	public static class MyMapKeyWordcountMap extends 
	//LongWritable 要处理的文件的每一行首字母在整个文件中的偏移量
	//Text 每一行数据
	//Text 单词
	//IntWritable 单词出现时所标识的 1
	//为什么使用mr特殊类型  --->  因为数据需要分布式交互,所以需要MR的包装类型
	
		Mapper<LongWritable , Text , MyText , IntWritable>{
		@Override
		/*
		 * map() 按照行循环调用
		 * 对每一行 切分并且把切分后的单词标一
		 */
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] splits = value.toString().split("\\s+");
			for (String str : splits) {
				context.write(new MyText(str), new IntWritable(1));
			}
		}
	}
	
	//------------------中间默认触发--按照key合并的功能-----------------------------
	
	public static class MyMapKeyWordcountReducer extends
	/*
	 * 1.Text  要和mapper的第三个参数一样
	 * 2.IntWritable  和mapper的第四个参数一样
	 * 3.Text (单词) String对应的类型
	 * 4.IntWritable (单词出现的次数) 整型
	 * reduce 按照key循环调用
	 * 把每一个单词对应的1的列表相加  得到所有这个单词的出现次数
	 */
		Reducer<MyText , IntWritable , MyText , IntWritable>{
		@Override
		protected void reduce(MyText key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws Exception {
		 //判断执行的命令是否满足需求,如少参数则退出,没有继续执行的必要
		if(args.length < 2){
			System.out.println("usage ... <in>...<out>");
			System.err.println("命令缺少参数....");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
		//一个计算就是一个Job
		Job job = Job.getInstance(conf);
		//设置主类
		job.setJarByClass(MyMapKeyWordcount.class);
		//设置运行程序的名称
		job.setJobName("MyMapKeyWordcount");
		//map输出的key的类型
		job.setMapOutputKeyClass(MyText.class);
		//map输出的value的类型
		job.setMapOutputValueClass(IntWritable.class);
		//reducer输出的key的类型
		job.setOutputKeyClass(MyText.class);
		//reducer输出的value的类型
		job.setOutputValueClass(IntWritable.class);
		//指定map类
		job.setMapperClass(MyMapKeyWordcountMap.class);
		//指定map类
		job.setReducerClass(MyMapKeyWordcountReducer.class);
//		job.setNumReduceTasks(0);
		 //要处理的文件的路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//处理的文件结果保存路径,此路径要不存在
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//启动计算等待计算 true 在终端显示运行详细信息  false不显示
		System.exit(job.waitForCompletion(false) ? 0 : 1);
	}
	
}
