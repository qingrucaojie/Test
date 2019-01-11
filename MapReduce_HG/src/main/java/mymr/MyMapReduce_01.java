package mymr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MyMapReduce_01 {
	public static void main(String[] args) throws Exception {
		if(args.length < 2){
			System.out.println("usage <in> ... <out>");
			System.err.println("必须有输入和输出路径");
			System.exit(2);//0 代表正常运行 非0都是非正常系统退出 
		}else{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);
			job.setJarByClass(MyMapReduce_01.class);
			job.setJobName("mywordcount");
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setMapperClass(MyMapReduce_01Map.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setReducerClass(MyMapReduce_01Reducer.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true)? 0 : 1);
		}
	}
}






