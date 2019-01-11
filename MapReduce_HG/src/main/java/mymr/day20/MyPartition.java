package mymr.day20;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartition extends Partitioner
					<IntWritable, NullWritable>{

	@Override
	/*
	 * IntWritable key : reducer输出的key
	 * NullWritable value : reducer输出的value
	 * numPartitions : 手动指定的reducerTask的数量
	 */
	public int getPartition(IntWritable key, 
				NullWritable value, int numPartitions) {
		int num = key.get();
		
		return num % numPartitions;
	}

}
