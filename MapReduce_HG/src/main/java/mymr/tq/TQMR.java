package mymr.tq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TQMR {
	
	
	public static void main(String[] args) throws Exception {
		
		
		
		//1,conf
		Configuration conf = new Configuration(true);
		
		//2,job
		Job job = Job.getInstance(conf);
		job.setJarByClass(TQMR.class);
		
		//3,input,output
		
		Path input = new Path(args[0]);
		FileInputFormat.addInputPath(job, input);
		
		Path output = new Path(args[1]);
		/*if(output.getFileSystem(conf).exists(output)){
			output.getFileSystem(conf).delete(output,true);
		}*/
		FileOutputFormat.setOutputPath(job, output );
		
		
		//4,map
		job.setMapperClass(TqMapper.class);
		job.setMapOutputKeyClass(TQ.class);
		job.setMapOutputValueClass(Text.class);
		
		
		//5,reduce
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(TqReducer.class);
//		job.setNumReduceTasks(2);
		
		//6,other:sort,part..,group...
//		job.setPartitionerClass(TqPartitioner.class);
//		job.setSortComparatorClass(TqSortComparator.class);
		job.setGroupingComparatorClass(TqGroupingComparator.class);
		
		/*job.setCombinerClass(TqReducer.class);
		job.setCombinerKeyGroupingComparatorClass(TqGroupingComparator.class);*/
		//7,submit
		
		job.waitForCompletion(true);
		
		
		
		
		
		
		
		
	}
	

}
