package modClasses;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class mapKeyPartitionerLongLongArray extends Partitioner<LongWritable,LongArrayWritable>{

	@Override
	public int getPartition(LongWritable key, LongArrayWritable value,
			int numPartitions) {
		return (((int) (key.get()/10)) % numPartitions);
	}
}
