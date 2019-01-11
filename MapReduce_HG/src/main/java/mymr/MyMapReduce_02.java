package mymr;

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
public class MyMapReduce_02 {

	/*
	 * mapper��
	 * ���ļ��е�ÿһ�����տո�ָ�ĵ����г�;ÿ�����ʱ� 1 
	 */
	public static class MyMapReduce_02Map extends 
	//LongWritable Ҫ������ļ���ÿһ������ĸ�������ļ��е�ƫ����
	//Text ÿһ������
	//Text ����
	//IntWritable ���ʳ���ʱ����ʶ�� 1
	//Ϊʲôʹ��mr��������  --->  ��Ϊ������Ҫ�ֲ�ʽ����,������ҪMR�İ�װ����
	
		Mapper<LongWritable , Text , Text , IntWritable>{
		Text k = new Text();//1��
		IntWritable v = new IntWritable(1);
		@Override
		/*
		 * map() ������ѭ������
		 * ��ÿһ�� �зֲ��Ұ��зֺ�ĵ��ʱ�һ
		 */
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] splits = value.toString().split(" ");
			for (String str : splits) {
				k.set(str);
				context.write(k , v);//ѭ�����ο����� * map���õĴ���
			}
		}
	}
	
	//------------------�м�Ĭ�ϴ���--����key�ϲ��Ĺ���-----------------------------
	
	public static class MyMapReduce_02Reducer extends
	/*
	 * 1.Text  Ҫ��mapper�ĵ���������һ��
	 * 2.IntWritable  ��mapper�ĵ��ĸ�����һ��
	 * 3.Text (����) String��Ӧ������
	 * 4.IntWritable (���ʳ��ֵĴ���) ����
	 * reduce ����keyѭ������
	 * ��ÿһ�����ʶ�Ӧ��1���б����  �õ�����������ʵĳ��ִ���
	 */
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
		job.setJarByClass(MyMapReduce_02.class);
		//�������г��������
		job.setJobName("MyMapReduce_02");
		//map�����key������
		job.setMapOutputKeyClass(Text.class);
		//map�����value������
		job.setMapOutputValueClass(IntWritable.class);
		//reducer�����key������
		job.setOutputKeyClass(Text.class);
		//reducer�����value������
		job.setOutputValueClass(IntWritable.class);
		//ָ��map��
		job.setMapperClass(MyMapReduce_02Map.class);
		//ָ��map��
		job.setReducerClass(MyMapReduce_02Reducer.class);
		 //Ҫ������ļ���·��
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//������ļ��������·��,��·��Ҫ������
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//��������ȴ����� true ���ն���ʾ������ϸ��Ϣ  false����ʾ
		System.exit(job.waitForCompletion(false) ? 0 : 1);
	}
	
}
