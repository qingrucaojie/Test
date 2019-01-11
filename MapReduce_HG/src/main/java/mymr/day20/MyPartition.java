package mymr.day20;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartition extends Partitioner
					<IntWritable, NullWritable>{

	@Override
	/*
	 * IntWritable key : reducer�����key
	 * NullWritable value : reducer�����value
	 * numPartitions : �ֶ�ָ����reducerTask������
	 */
	public int getPartition(IntWritable key, 
				NullWritable value, int numPartitions) {
		int num = key.get();
		
		return num % numPartitions;
	}

}
