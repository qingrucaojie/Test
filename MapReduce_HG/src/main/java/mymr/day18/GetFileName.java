package mymr.day18;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class GetFileName extends Configured implements Tool{

	
	public static class GetFileNameMap extends Mapper
		<LongWritable , Text , Text , Text>{//������������ҵ����Textռλ
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			//setup()����map()֮ǰ����һ��
		}
		@Override
		protected void map(LongWritable key, Text value, 
				Context context)
				throws IOException, InterruptedException {
			//\t tab��  \n ���� \\s+(����Ŀհ��ַ�)
			String[] splits = value.toString().split("\\s+");
			FileSplit filesplit = (FileSplit)context.getInputSplit();
			String fileName = filesplit.getPath().getName();
			context.write(new Text(splits[0]), 
					new Text(fileName + " : " +splits[1]));
			
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			//cleanup������map�������ý�������һ��
		}
	}
	//����ļ�����  ֻ��ָ���ļ�Ŀ¼����
	public static class GetFileNameReducer extends Reducer
		<Text , Text , Text , Text>{
		@Override
		protected void setup(Context context) 
				throws IOException, InterruptedException {
			
		}
		//Iterable<Text> values  ѧ����Ӧ�����ųɼ��ļ���
		@Override
		protected void reduce(Text key, Iterable<Text> values, 
				Context context)
				throws IOException, InterruptedException {
			String str = "";
			for (Text value : values) {
				str += value + ";" ;
			}
			context.write(key, new Text(str));
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			
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
		job.setJobName("GetFileName");
		job.setJarByClass(GetFileName.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(GetFileNameMap.class);
		job.setReducerClass(GetFileNameReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1 ;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new GetFileName(), args));
	}


}
