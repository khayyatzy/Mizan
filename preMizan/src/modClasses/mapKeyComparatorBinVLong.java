package modClasses;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.io.RawComparator;

public class mapKeyComparatorBinVLong implements RawComparator<VLongWritable>{

	@Override
	public int compare(VLongWritable o1, VLongWritable o2) {
		// TODO Auto-generated method stub
		return ((o1.get()/2)<(o2.get()/2) ? -1 : ((o1.get()/2)==(o2.get()/2) ? 0 : 1));
	}
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		long thisValue = WritableComparator.readLong(b1, s1)/2;
	    long thatValue = WritableComparator.readLong(b2, s2)/2;
	    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
	}

}
