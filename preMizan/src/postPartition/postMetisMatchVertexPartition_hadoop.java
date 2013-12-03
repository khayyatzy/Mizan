package postPartition;

import java.io.IOException;
import java.util.StringTokenizer;

import modClasses.LongArrayWritable;
import modClasses.LongArrayWriter;
import modClasses.mapKeyComparatorBinLong;
import modClasses.mapKeyPartitionerBinLongLongArray;
import modClasses.noKeyLongWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Input graph as edge list (src dst) & vertex assignment ID (vertex SGID)
//output 1 as (src SGID dst)
//output 2 as (stc SGID dst SGID)

public class postMetisMatchVertexPartition_hadoop {

	// Part A
	public static class matchVertexMapperA extends
			Mapper<Object, Text, LongWritable, LongArrayWritable> {
		// input: (src dst)
		// output:

		int edgeWeight;
		boolean isGraphPartitioned;
		long disp;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.disp = Integer.parseInt(context.getConfiguration().get(
					"PARTID_DISPLACEMENT"));
			this.edgeWeight = Integer.parseInt(context.getConfiguration().get(
					"EDGE_WEIGHT"));
			this.isGraphPartitioned = Boolean.parseBoolean(context.getConfiguration().get(
					"GRAPH_PARTITIONED"));
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String input = value.toString();
			StringTokenizer st = new StringTokenizer(value.toString(), " \t");
			if (!input.startsWith("#") && input.trim().length() != 0) {
				if (st.countTokens() == 3) {
					// the input is from postMetisVertexAssignID_hadoop
					st.nextToken();
					long tmpKey = (Long.parseLong(st.nextToken()) - disp) * 2;
					long subGLoc = Long.parseLong(st.nextToken());
					LongWritable keyA = new LongWritable(tmpKey);
					LongArrayWritable valueA = new LongArrayWritable();
					LongWritable[] valueAArray = new LongWritable[2];
					valueAArray[0] = new LongWritable(subGLoc);
					valueAArray[1] = new LongWritable(0);
					valueA.set(valueAArray);
					context.write(keyA, valueA);
				} else {
					// the input is from the original File
					int size = st.countTokens();
					LongWritable[] valueBArray;
					long srcKey;
					if(isGraphPartitioned){
						valueBArray = new LongWritable[size];	
						int i=0;
						while(st.hasMoreTokens()){
							valueBArray[i] = new LongWritable(Long.parseLong(st.nextToken()));
							i++;
						}
						//key is the src partID
						srcKey = (valueBArray[1].get() * 2) + 1;
					}
					else{
						valueBArray = new LongWritable[this.edgeWeight+4];
						//Assign source
						valueBArray[0] = new LongWritable((Long.parseLong(st.nextToken())));
						valueBArray[1] = new LongWritable(-1);
						//Assign weights and destination
						int i=2;
						while(st.hasMoreTokens()){
							valueBArray[i] = new LongWritable(Long.parseLong(st.nextToken()));
							i++;
						}
						valueBArray[i] = new LongWritable(-1);
						
						//key is the src
						srcKey = (valueBArray[0].get() * 2) + 1;
					}
					
				
					LongWritable keyB = new LongWritable(srcKey);
					LongArrayWritable valueB = new LongArrayWritable();
					
					valueB.set(valueBArray);
					context.write(keyB, valueB);
				}
			}
		}
	}

	public static class matchVertexReducerA
			extends
			Reducer<LongWritable, LongArrayWritable, LongWritable, LongArrayWritable> {
		// input
		// output
		public void reduce(LongWritable key,
				Iterable<LongArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			long verPartID = -1;
			long vertexID = key.get() / 2;
			for (LongArrayWritable inValue : values) {
				if (inValue.get().length == 2) {
					verPartID = ((LongWritable) inValue.get()[0]).get();
				} else {
					LongWritable newKey = new LongWritable(vertexID);
					LongArrayWritable newVal = inValue;
					
					Writable[] newValArray = newVal.get();
					newValArray[1] = new LongWritable(verPartID);
			
					newVal.set(newValArray);
					context.write(newKey, newVal);
				}
			}
		}
	}

	// Part B
	public static class matchVertexMapperB extends
			Mapper<Object, Text, LongWritable, LongArrayWritable> {
		// input: (src dst)
		// output:
		int edgeWeight;
		boolean isGraphPartitioned;
		long disp;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.disp = Integer.parseInt(context.getConfiguration().get(
					"PARTID_DISPLACEMENT"));
			this.edgeWeight = Integer.parseInt(context.getConfiguration().get(
					"EDGE_WEIGHT"));
			this.isGraphPartitioned = Boolean.parseBoolean(context.getConfiguration().get(
					"GRAPH_PARTITIONED"));
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String input = value.toString();
			StringTokenizer st = new StringTokenizer(value.toString(), " \t");
			if (!input.startsWith("#") && input.trim().length() != 0) {
				if (st.countTokens() == 3) {
					// the input is from postMetisVertexAssignID_hadoop
					st.nextToken();
					long tmpKey = (Long.parseLong(st.nextToken()) - disp) * 2;
					long subGLoc = Long.parseLong(st.nextToken());
					LongWritable keyA = new LongWritable(tmpKey);
					LongArrayWritable valueA = new LongArrayWritable();
					LongWritable[] valueAArray = new LongWritable[2];
					valueAArray[0] = new LongWritable(subGLoc);
					valueAArray[1] = new LongWritable(0);
					valueA.set(valueAArray);
					context.write(keyA, valueA);
				} else {
					// the input is from the original File
					int size = st.countTokens();
					LongWritable[] valueBArray;
					long srcKey;
					
					valueBArray = new LongWritable[size];	
					int i=0;
					while(st.hasMoreTokens()){
						valueBArray[i] = new LongWritable(Long.parseLong(st.nextToken()));
						i++;
					}
					
					if(isGraphPartitioned){
						//key is the dst partID
						//long dstKey = (dst * 2) + 1;
						srcKey = (valueBArray[i-1].get() * 2) + 1;
					}
					else{
						//key is the dst
						srcKey = (valueBArray[i-2].get() * 2) + 1;
					}
					
				
					LongWritable keyB = new LongWritable(srcKey);
					LongArrayWritable valueB = new LongArrayWritable();
					
					valueB.set(valueBArray);
					context.write(keyB, valueB);
				}
			}
		}
	}

	public static class matchVertexReducerB
			extends
			Reducer<LongWritable, LongArrayWritable, LongWritable, LongArrayWritable> {
		// input
		// output
		public void reduce(LongWritable key,
				Iterable<LongArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			long verPartID = -1;
			long vertexID = key.get() / 2;
			for (LongArrayWritable inValue : values) {
				if (inValue.get().length == 2) {
					verPartID = ((LongWritable) inValue.get()[0]).get();
				} else {
					LongWritable newKey = new LongWritable(vertexID);
					LongArrayWritable newVal = inValue;
					
					Writable[] newValArray = newVal.get();
					newValArray[3] = new LongWritable(verPartID);
			
					newVal.set(newValArray);
					context.write(newKey, newVal);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration confA = new Configuration();
		confA.setQuietMode(true);

		confA.set("PARTID_DISPLACEMENT", 1 + "");
		confA.set("EDGE_WEIGHT", args[3]);
		confA.set("GRAPH_PARTITIONED","false");
		
		Job jobA = new Job(confA, "5a- postMetisVertexMatch");

		jobA.setOutputFormatClass(noKeyLongWriter.class);

		jobA.setPartitionerClass(mapKeyPartitionerBinLongLongArray.class);
		jobA.setGroupingComparatorClass(mapKeyComparatorBinLong.class);
		// jobA.setSortComparatorClass(LongWritable.Comparator.class);

		jobA.setJarByClass(postMetisMatchVertexPartition_hadoop.class);
		jobA.setMapperClass(matchVertexMapperA.class);
		jobA.setReducerClass(matchVertexReducerA.class);

		jobA.setOutputKeyClass(LongWritable.class);
		jobA.setOutputValueClass(LongArrayWritable.class);
		FileInputFormat.addInputPaths(jobA, args[0] + "," + args[1]);
		FileOutputFormat.setOutputPath(jobA, new Path(args[2] + "_a"));
		// job.setNumReduceTasks(5);
		// submit and wait
		jobA.waitForCompletion(false);

		// Starting Part B
		Configuration confB = new Configuration();
		confB.setQuietMode(true);
		
		confB.set("PARTID_DISPLACEMENT", 1 + "");
		confB.set("EDGE_WEIGHT",args[3]);
		confB.set("GRAPH_PARTITIONED","false");

		Job jobB = new Job(confB, "5b- postMetisVertexMatch");

		jobB.setOutputFormatClass(noKeyLongWriter.class);

		jobB.setPartitionerClass(mapKeyPartitionerBinLongLongArray.class);
		jobB.setGroupingComparatorClass(mapKeyComparatorBinLong.class);
		jobB.setSortComparatorClass(LongWritable.Comparator.class);

		jobB.setJarByClass(postMetisMatchVertexPartition_hadoop.class);
		jobB.setMapperClass(matchVertexMapperB.class);
		jobB.setReducerClass(matchVertexReducerB.class);

		jobB.setOutputKeyClass(LongWritable.class);
		jobB.setOutputValueClass(LongArrayWritable.class);
		FileInputFormat.addInputPaths(jobB, args[1] + "," + args[2] + "_a");
		FileOutputFormat.setOutputPath(jobB, new Path(args[2] + "_b"));
		// job.setNumReduceTasks(5);
		// submit and wait
		System.exit(jobB.waitForCompletion(false) ? 0 : 1);
	}
}
