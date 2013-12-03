package modClasses;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class mapKeyPartitionerIntVLongArray extends Partitioner<IntWritable,VLongArrayWritable>{

	@Override
	public int getPartition(IntWritable key, VLongArrayWritable value,
			int numPartitions) {
		return (((int) (key.get()/10)) % numPartitions);
	}
}
