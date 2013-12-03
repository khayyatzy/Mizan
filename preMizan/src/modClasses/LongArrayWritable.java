package modClasses;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

public class LongArrayWritable extends ArrayWritable {
	public LongArrayWritable() { 
		super(LongWritable.class); 
	}
	/*
	public String toString(){
		String output = "";
		for (int i=0;i<this.get().length;i++){
			output = output + this.get()[i] + " ";
		}
		return output.trim();
	}
	*/
}
