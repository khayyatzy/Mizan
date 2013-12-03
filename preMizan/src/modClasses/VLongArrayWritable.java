package modClasses;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.VLongWritable;

public class VLongArrayWritable extends ArrayWritable {
	public VLongArrayWritable() { 
		super(VLongWritable.class); 
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
