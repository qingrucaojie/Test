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
 * ��Ƶͳ�Ƶ�����
 */
public class MyMapKeyWordcount {

	/*
	 * mapper��
	 * ���ļ��е�ÿһ�����տո�ָ�ĵ����г�;ÿ�����ʱ� 1 
	 */
	public static class MyMapKeyWordcountMap extends 
	//LongWritable Ҫ������ļ���ÿһ������ĸ�������ļ��е�ƫ����
	//Text ÿһ������
	//Text ����
	//IntWritable ���ʳ���ʱ����ʶ�� 1
	//Ϊʲôʹ��mr��������  --->  ��Ϊ������Ҫ�ֲ�ʽ����,������ҪMR�İ�װ����
	
		Mapper<LongWritable , Text , MyText , IntWritable>{
		@Override
		/*
		 * map() ������ѭ������
		 * ��ÿһ�� �зֲ��Ұ��зֺ�ĵ��ʱ�һ
		 */
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] splits = value.toString().split("\\s+");
			for (String str : splits) {
				context.write(new MyText(str), new IntWritable(1));
			}
		}
	}
	
	//------------------�м�Ĭ�ϴ���--����key�ϲ��Ĺ���-----------------------------
	
	public static class MyMapKeyWordcountReducer extends
	/*
	 * 1.Text  Ҫ��mapper�ĵ���������һ��
	 * 2.IntWritable  ��mapper�ĵ��ĸ�����һ��
	 * 3.Text (����) String��Ӧ������
	 * 4.IntWritable (���ʳ��ֵĴ���) ����
	 * reduce ����keyѭ������
	 * ��ÿһ�����ʶ�Ӧ��1���б����  �õ�����������ʵĳ��ִ���
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
		 //�ж�ִ�е������Ƿ���������,���ٲ������˳�,û�м���ִ�еı�Ҫ
		if(args.length < 2){
			System.out.println("usage ... <in>...<out>");
			System.err.println("����ȱ�ٲ���....");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
		//һ���������һ��Job
		Job job = Job.getInstance(conf);
		//��������
		job.setJarByClass(MyMapKeyWordcount.class);
		//�������г��������
		job.setJobName("MyMapKeyWordcount");
		//map�����key������
		job.setMapOutputKeyClass(MyText.class);
		//map�����value������
		job.setMapOutputValueClass(IntWritable.class);
		//reducer�����key������
		job.setOutputKeyClass(MyText.class);
		//reducer�����value������
		job.setOutputValueClass(IntWritable.class);
		//ָ��map��
		job.setMapperClass(MyMapKeyWordcountMap.class);
		//ָ��map��
		job.setReducerClass(MyMapKeyWordcountReducer.class);
//		job.setNumReduceTasks(0);
		 //Ҫ������ļ���·��
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//������ļ��������·��,��·��Ҫ������
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//��������ȴ����� true ���ն���ʾ������ϸ��Ϣ  false����ʾ
		System.exit(job.waitForCompletion(false) ? 0 : 1);
	}
	
}
