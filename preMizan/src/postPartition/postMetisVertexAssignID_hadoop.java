package postPartition;

import java.io.IOException;

import modClasses.LongArrayWriter;
import modClasses.VLongArrayWritable;
import modClasses.mapKeyComparatorInt;
import modClasses.mapKeyPartitionerIntVLongArray;
import modClasses.noKeyLongWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Input metis output file (partID)
//output (vertex partID)

public class postMetisVertexAssignID_hadoop {	
	public static class subGraphAssignMapper extends Mapper<LongWritable, Text, IntWritable, VLongArrayWritable>{
		
		//long mapCount = 0;
		static long sizeCount = 0;
		static long lastCnt = 0;
		
		//input (splitID)
		//output A: ((MapID*10+2) (vertexID splitID)) ==> send the vertexID and splitID to reducer (MapID*10) with order 2
		// 		 B: (((i)*10+1) lastCount lastSize) ==> send the lastCount & lastSize of (MapID) to all reducers with order 1
		//		 C:	(myID*10 lastCount lastSize) ==> send to the reducer with ID (MapID) lastCount & lastSize with order 0
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			int myID = Integer.parseInt(context.getTaskAttemptID().toString().split("_")[4]);
			
			//Local line ID ==> vertexID
			long vertexID = (key.get()+1)-sizeCount;
			//System.out.println(myID+": "+key.get()+" ==> "+value.toString());
			sizeCount = sizeCount + value.toString().length(); 
			
			//Sending output A
			IntWritable outKeyA = new IntWritable((myID*10)+2);
			VLongArrayWritable outValA = new VLongArrayWritable();
			VLongWritable[] outValArrayA = new VLongWritable[2];
			outValArrayA[0] = new VLongWritable(vertexID);
			outValArrayA[1] = new VLongWritable(Long.parseLong(value.toString()));
			outValA.set(outValArrayA);	
			context.write(outKeyA, outValA);
			
			lastCnt = vertexID;
			//System.out.println(sizeCount+" -- "+context.getInputSplit().getLength()+" -- "+context.getInputSplit().getLocations().length);
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
			int myID = Integer.parseInt(context.getTaskAttemptID().toString().split("_")[4]);
			int mapCount = Integer.parseInt(context.getConfiguration().get("mapred.map.tasks"));
			System.out.println("mapCount = "+mapCount);
			//Sending output B:	(((MapID+1)*10) lastSize)
			for (int i=0;i<mapCount;i++){
				IntWritable outKeyB = new IntWritable((i)*10+1);
				VLongArrayWritable outValB = new VLongArrayWritable();
				VLongWritable[] outValArrayB = new VLongWritable[3];
				outValArrayB[0] = new VLongWritable(lastCnt);
				outValArrayB[1] = new VLongWritable(sizeCount);
				outValArrayB[2] = new VLongWritable(0);
				outValB.set(outValArrayB);
				context.write(outKeyB, outValB);
			}
			
			//Sending output C
			IntWritable outKeyC = new IntWritable((myID*10));
			VLongArrayWritable outValC = new VLongArrayWritable();
			VLongWritable[] outValArrayC = new VLongWritable[3];
			outValArrayC[0] = new VLongWritable(lastCnt);
			outValArrayC[1] = new VLongWritable(sizeCount);
			outValArrayC[2] = new VLongWritable(0);
			outValC.set(outValArrayC);
			context.write(outKeyC, outValC);
			
		}
	}
	
	public static class subGraphAssignReducer extends Reducer<IntWritable,VLongArrayWritable,IntWritable,VLongArrayWritable> {
		public void reduce(IntWritable key, Iterable<VLongArrayWritable> values, Context context) throws IOException, InterruptedException {
			//long lastSize = 0;
			long ownLastCount = 0;
			long curLastCount = 0;
			long offset=0;
			int order = 0;
			//System.out.println("=================");
			for (VLongArrayWritable outVal:values){
				//System.out.println(key.get()+" - "+outVal.toString());
				if(outVal.get().length!=2){
					if(order==0){
						//wait for Map output C
						ownLastCount = ((VLongWritable)outVal.get()[0]).get();
						//lastSize = ((VLongWritable)outVal.get()[1]).get();
						order++;
						//System.out.println("ownLastCount = "+ownLastCount+" lastSize ="+lastSize);
					}
					else{
						//wait for Map output B
						curLastCount = ((VLongWritable)outVal.get()[0]).get();
						if(ownLastCount > curLastCount){
							offset = offset + ((VLongWritable)outVal.get()[1]).get();
							//System.out.println("ownLastCount = "+ownLastCount+" curLastCount ="+curLastCount + " offset = "+offset);
							
						}
					}
				}
				else{
					//wait for Map output A
					VLongArrayWritable newOutVal = new VLongArrayWritable();
					VLongWritable[] newOutValArray = new VLongWritable[2];
					newOutValArray[0] = new VLongWritable(((VLongWritable)outVal.get()[0]).get()-offset -1);
					newOutValArray[1] = new VLongWritable(((VLongWritable)outVal.get()[1]).get());
					newOutVal.set(newOutValArray);
					context.write(key, newOutVal);
				}
			}
			//System.out.println("-----------------");
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    conf.setQuietMode(true);
	    
	    /*
	    //Setting the number of input Mappers
	    FileSystem hdfs = FileSystem.get(conf);
	    Path inputPath = new Path(args[0]);
	    FileStatus fst = hdfs.getFileStatus(inputPath);
	    long calcMapCount = 0;
	    double blockSize = 0;
	    double fileSize = 0;
	    blockSize = fst.getBlockSize();
	    fileSize = fst.getLen();
	    calcMapCount = calcMapCount + (long)(Math.ceil(fileSize/blockSize));
	    	
	    System.out.println("calcMapCount = "+calcMapCount);
	    conf.set("MAP_COUNT", ((long)calcMapCount)+"");
	    */
	    
	    Job job = new Job(conf, "4- postMVertexAssignID");
	    
	    job.setPartitionerClass(mapKeyPartitionerIntVLongArray.class);
	    job.setGroupingComparatorClass(mapKeyComparatorInt.class);
	    job.setSortComparatorClass(IntWritable.Comparator.class);
	    
	    job.setJarByClass(postMetisVertexAssignID_hadoop.class);
	    job.setMapperClass(subGraphAssignMapper.class);
	    //job.setCombinerClass(subGraphAssignCombiner.class);
	    job.setReducerClass(subGraphAssignReducer.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(LongArrayWriter.class);
	    
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(VLongArrayWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    //job.setNumReduceTasks(1);
	    //submit and wait
	    System.exit(job.waitForCompletion(false) ? 0 : 1);
	}
}