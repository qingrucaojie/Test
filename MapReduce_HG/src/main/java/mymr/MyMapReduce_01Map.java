package mymr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapReduce_01Map extends 
		Mapper<LongWritable , Text , Text , IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] splits = value.toString().split(" ");
		for (String str : splits) {
			context.write(new Text(str), new IntWritable(1));
		}
	}
}
