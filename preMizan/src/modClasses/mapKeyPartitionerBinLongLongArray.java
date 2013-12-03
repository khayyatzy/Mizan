package modClasses;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class mapKeyPartitionerBinLongLongArray extends Partitioner<LongWritable,LongArrayWritable>{

	@Override
	public int getPartition(LongWritable key, LongArrayWritable value,
			int numPartitions) {
		return (((int) (key.get()/2)) % numPartitions);
	}
}
