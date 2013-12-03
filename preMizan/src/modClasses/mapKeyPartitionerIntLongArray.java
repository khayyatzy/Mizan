package modClasses;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class mapKeyPartitionerIntLongArray extends Partitioner<IntWritable,LongArrayWritable>{

	@Override
	public int getPartition(IntWritable key, LongArrayWritable value,
			int numPartitions) {
		return (((int) (key.get()/10)) % numPartitions);
	}
}
