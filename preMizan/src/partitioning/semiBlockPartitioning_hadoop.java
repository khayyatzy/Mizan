package partitioning;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

import modClasses.LongArrayWriter;
import modClasses.VLongArrayWritable;
import modClasses.noKeyLongWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import prePartition.preMetisCountAppend_hadoop;
import prePartition.preMetisCountAppend_hadoop.FixEmptyVertexCombiner;
import prePartition.preMetisCountAppend_hadoop.FixEmptyVertexMapper;
import prePartition.preMetisCountAppend_hadoop.listCounterCombiner;
import prePartition.preMetisCountAppend_hadoop.listCounterMapper;
import prePartition.preMetisCountAppend_hadoop.listCounterReducer;
import prePartition.preMetisCountAppend_hadoop.mapRedMessage;

//A
//Input: Mizan SubGraph file format (src srcLoc [Weight] dst dstLoc)
//Output: (srcLoc (dstLoc sum(EdgeWeight)/2)) ==> sum(EdgeWeight)/2 is because of Mizan's info duplication in srcLoc and dstLoc

//B
//Input: (srcLoc (dstLoc sumEdgesWeight)))
//Output: 

public class semiBlockPartitioning_hadoop {

	// input SubGraph (src srcLoc dst dstLoc)
	// output (srcLoc (dstLoc EdgeWeight)) ==> default EdgeWeight = 1
	public static class semiBlockSummeryMapper extends
			Mapper<Object, Text, IntWritable, VLongArrayWritable> {
		String inputName;
		int disp;
		int edgeWeight;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.inputName = context.getConfiguration().get("map.input.file");
			this.disp = Integer.parseInt(context.getConfiguration().get(
					"PARTID_DISPLACEMENT"));
			this.edgeWeight = Integer.parseInt(context.getConfiguration().get(
					"EDGE_WEIGHT"));
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// System.out.println("inputName = "+inputName);
			int outEdgeWeight = 1;
			StringTokenizer st = new StringTokenizer(value.toString(), " \t");

			long srcVertex = Long.parseLong(st.nextToken());
			int srcLocation = Integer.parseInt(st.nextToken()) + disp;
			if (this.edgeWeight > 0) {
				outEdgeWeight = this.edgeWeight;
			}
			long dstVertex = Long.parseLong(st.nextToken());
			int dstLocation = Integer.parseInt(st.nextToken()) + disp;

			IntWritable outKey = new IntWritable(srcLocation);
			VLongArrayWritable outValue = new VLongArrayWritable();
			VLongWritable[] outValueArray = new VLongWritable[2];
			outValueArray[0] = new VLongWritable(dstLocation);
			outValueArray[1] = new VLongWritable(outEdgeWeight);

			outValue.set(outValueArray);

			context.write(outKey, outValue);
		}
	}

	// input (srcLoc (dstLoc EdgeWeight))
	// output (srcLoc (dstLoc pSum(EdgeWeight)))
	public static class semiBlockSummeryCombiner
			extends
			Reducer<IntWritable, VLongArrayWritable, IntWritable, VLongArrayWritable> {

		public void reduce(IntWritable key,
				Iterable<VLongArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			HashMap<Integer, Long> tmpData = new HashMap<Integer, Long>();
			IntWritable outKey = new IntWritable(key.get());
			for (VLongArrayWritable inVals : values) {
				// int srcVertex = (int)((VLongWritable)inVals.get()[0]).get();
				int dstVertex = (int) ((VLongWritable) inVals.get()[0]).get();
				long dstWeight = ((VLongWritable) inVals.get()[1]).get();
				Long count;
				if ((count = tmpData.get(dstVertex)) != null) {
					count = new Long(count.longValue() + dstWeight);
					tmpData.put(dstVertex, count);
				} else {
					tmpData.put(dstVertex, new Long(dstWeight));
				}
			}

			for (int tmpKey : tmpData.keySet()) {
				VLongArrayWritable outValue = new VLongArrayWritable();
				VLongWritable[] outValueArray = new VLongWritable[2];
				if (tmpKey != outKey.get()) {
					outValueArray[0] = new VLongWritable(tmpKey);
					outValueArray[1] = new VLongWritable(tmpData.get(tmpKey));
				} else {
					outValueArray[0] = new VLongWritable(tmpKey);
					outValueArray[1] = new VLongWritable(0);//new VLongWritable(tmpData.get(tmpKey));
				}
				outValue.set(outValueArray);
				context.write(outKey, outValue);
			}
		}
	}

	// input (srcLoc (dstLoc pSum(EdgeWeight)))
	// output (srcLoc (dstLoc sum(EdgeWeight))) && (srcLoc (srcLoc 0)) if no
	// srcLoc EdgeWeight exists
	public static class semiBlockSummeryReduce
			extends
			Reducer<IntWritable, VLongArrayWritable, IntWritable, VLongArrayWritable> {

		public void reduce(IntWritable key,
				Iterable<VLongArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			HashMap<Integer, Long> tmpData = new HashMap<Integer, Long>();
			IntWritable outKey = new IntWritable(key.get());

			for (VLongArrayWritable inVals : values) {
				// int srcVertex = (int)((VLongWritable)inVals.get()[0]).get();
				int dstVertex = (int) ((VLongWritable) inVals.get()[0]).get();
				long dstWeight = ((VLongWritable) inVals.get()[1]).get();
				Long count;
				if ((count = tmpData.get(dstVertex)) != null) {
					count = new Long(count.longValue() + dstWeight);
					tmpData.put(dstVertex, count);
				} else {
					tmpData.put(dstVertex, new Long(dstWeight));
				}
			}
			boolean hasEdges = false;
			for (int tmpKey : tmpData.keySet()) {
				VLongArrayWritable outValue = new VLongArrayWritable();
				VLongWritable[] outValueArray = new VLongWritable[2];
				outValueArray[0] = new VLongWritable(tmpKey);
				outValueArray[1] = new VLongWritable(tmpData.get(tmpKey));
				if (tmpKey == outKey.get()) {
					hasEdges = true;
				}
				outValue.set(outValueArray);
				context.write(outKey, outValue);
			}
			if (!hasEdges) {
				VLongArrayWritable outValue = new VLongArrayWritable();
				VLongWritable[] outValueArray = new VLongWritable[2];
				outValueArray[0] = new VLongWritable(outKey.get());
				outValueArray[1] = new VLongWritable(0);
				outValue.set(outValueArray);
				context.write(outKey, outValue);
			}
		}
	}

	// ***************************************************
	// B
	// Input: (srcLoc (dstLoc sumEdgesWeight)))
	// Output: (srcLoc (dstLoc sumEdgesWeight))) && (dstLoc (srcLoc
	// sumEdgesWeight))) && (dstLoc (dstLoc 0)))
	public static class semiBlockMetisFormatMapper extends
			Mapper<Object, Text, IntWritable, VLongArrayWritable> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			StringTokenizer st = new StringTokenizer(value.toString());
			int srcLoc = Integer.parseInt(st.nextToken());
			int dstLoc = Integer.parseInt(st.nextToken());
			long srcDstWeight = Long.parseLong(st.nextToken());

			IntWritable outKeyA = new IntWritable(srcLoc);
			VLongArrayWritable outValueA = new VLongArrayWritable();
			VLongWritable[] outValueArrayA = new VLongWritable[2];
			outValueArrayA[0] = new VLongWritable(dstLoc);
			outValueArrayA[1] = new VLongWritable(srcDstWeight);
			outValueA.set(outValueArrayA);

			context.write(outKeyA, outValueA);

			if (srcLoc != dstLoc) {
				IntWritable outKeyB = new IntWritable(dstLoc);
				VLongArrayWritable outValueB = new VLongArrayWritable();
				VLongWritable[] outValueArrayB = new VLongWritable[2];
				outValueArrayB[0] = new VLongWritable(srcLoc);
				outValueArrayB[1] = new VLongWritable(srcDstWeight);
				outValueB.set(outValueArrayB);

				context.write(outKeyB, outValueB);

				IntWritable outKeyC = new IntWritable(dstLoc);
				VLongArrayWritable outValueC = new VLongArrayWritable();
				VLongWritable[] outValueArrayC = new VLongWritable[2];
				outValueArrayC[0] = new VLongWritable(dstLoc);
				outValueArrayC[1] = new VLongWritable(0);
				outValueC.set(outValueArrayC);

				context.write(outKeyC, outValueC);
			}
		}
	}

	// Input: (srcLoc (dstLoc sumEdgesWeight)))
	// Output: (srcLoc (dstLoc pSum(sumEdgesWeight))))
	public static class semiBlockMetisFormatCombiner
			extends
			Reducer<IntWritable, VLongArrayWritable, IntWritable, VLongArrayWritable> {
		public void reduce(IntWritable key,
				Iterable<VLongArrayWritable> values, Context context)
				throws IOException, InterruptedException {

			HashMap<Integer, Long> edgeWeight = new HashMap<Integer, Long>();

			IntWritable outKey = new IntWritable(key.get());
			for (VLongArrayWritable inVals : values) {
				Long tmpKey;
				int inVertex = (int) ((VLongWritable) inVals.get()[0]).get();
				long verWeight = ((VLongWritable) inVals.get()[1]).get();
				if ((tmpKey = edgeWeight.get(inVertex)) != null) {
					edgeWeight.put(inVertex, verWeight + tmpKey.longValue());
				} else {
					edgeWeight.put(inVertex, verWeight);
				}
			}

			for (int vertex : edgeWeight.keySet()) {
				VLongArrayWritable outValue = new VLongArrayWritable();
				VLongWritable[] outValueArray = new VLongWritable[2];
				outValueArray[0] = new VLongWritable(vertex);
				outValueArray[1] = new VLongWritable(edgeWeight.get(vertex));
				outValue.set(outValueArray);

				context.write(outKey, outValue);
			}
		}
	}

	// Input: (srcLoc (dstLoc sumEdgesWeight)))
	// Output: (srcLoc (dstLoc1 Sum1(sumEdgesWeight) dstLoc2
	// Sum2(sumEdgesWeight) ...)))
	public static class semiBlockMetisFormatReducer
			extends
			Reducer<IntWritable, VLongArrayWritable, IntWritable, VLongArrayWritable> {
		public void reduce(IntWritable key,
				Iterable<VLongArrayWritable> values, Context context)
				throws IOException, InterruptedException {

			HashMap<Integer, Long> edgeWeight = new HashMap<Integer, Long>();

			IntWritable outKey = new IntWritable(key.get());
			for (VLongArrayWritable inVals : values) {
				Long tmpKey;
				int inVertex = (int) ((VLongWritable) inVals.get()[0]).get();
				long verWeight = ((VLongWritable) inVals.get()[1]).get();

				if ((tmpKey = edgeWeight.get(inVertex)) != null) {
					edgeWeight.put(inVertex, verWeight + tmpKey.longValue());
				} else {
					edgeWeight.put(inVertex, verWeight);
				}
			}

			VLongArrayWritable outValue = new VLongArrayWritable();
			VLongWritable[] outValueArray = new VLongWritable[edgeWeight.size() * 2 - 1];

			int i = 1;
			for (int vertex : edgeWeight.keySet()) {
				if (vertex != outKey.get()) {
					outValueArray[i++] = new VLongWritable(vertex);
					outValueArray[i++] = new VLongWritable(edgeWeight.get(vertex));
				} else {
					//outValueArray[0] = new VLongWritable(edgeWeight.get(vertex));
					//Assume minimum vertex Weight = 0
					outValueArray[0] = new VLongWritable(Math.max(edgeWeight.get(vertex),1));
				}
			}
			outValue.set(outValueArray);
			context.write(outKey, outValue);
		}
	}

	// ***************************************************************
	/*
	 * public static class semiBlockListCounterMapper extends Mapper<Object,
	 * Text, VLongWritable, VLongArrayWritable> { // input: (src count dst1 dst2
	 * .. dstx) // output: (src dst1 dst2 .. dstx) & (0 max count)
	 * 
	 * public void map(Object key, Text value, Context context) throws
	 * IOException, InterruptedException {
	 * 
	 * String input = value.toString(); StringTokenizer st = new
	 * StringTokenizer(input, " \t");
	 * 
	 * int count = st.countTokens() - 1; // value = (dst1 dst2 .. dstx)
	 * VLongArrayWritable outValue1 = new VLongArrayWritable(); VLongWritable[]
	 * adjList = new VLongWritable[count];
	 * 
	 * // value = (max count) VLongArrayWritable outValue2 = new
	 * VLongArrayWritable(); VLongWritable[] max_count = new VLongWritable[2];
	 * 
	 * VLongWritable outKey1 = new VLongWritable(Long.parseLong(st
	 * .nextToken())); VLongWritable outKey2 = new VLongWritable(0);
	 * 
	 * // read (src) // OPT1 max_count[1] = new VLongWritable((count - 1) / 2);
	 * // read (count)
	 * 
	 * // Assume the src is the maximum long max = outKey1.get();
	 * 
	 * for (int i = 0; st.hasMoreTokens(); i++) { adjList[i] = new
	 * VLongWritable(Long.parseLong(st.nextToken())); if ((i & 0x0001) == 1) {
	 * max = Math.max(max, adjList[i].get()); } } max_count[0] = new
	 * VLongWritable(max);
	 * 
	 * outValue1.set(adjList); outValue2.set(max_count);
	 * 
	 * // output: (src dst1 dst2 .. dstx) context.write(outKey1, outValue1);
	 * 
	 * // output: (0 max count) context.write(outKey2, outValue2); } }
	 * 
	 * public static class semiBlockListCounterCombiner extends
	 * Reducer<VLongWritable, VLongArrayWritable, VLongWritable,
	 * VLongArrayWritable> { // input (0 max count) || (src dst1 dst2 .. dstx)
	 * // output (0 max count) || (src dst1 dst2 .. dstx) public void
	 * reduce(VLongWritable key, Iterable<VLongArrayWritable> values, Context
	 * context) throws IOException, InterruptedException {
	 * 
	 * if (key.get() == 0) { VLongArrayWritable outValue2 = new
	 * VLongArrayWritable(); VLongWritable[] max_count = new VLongWritable[2];
	 * 
	 * long max = 0; long count = 0; for (VLongArrayWritable value : values) {
	 * max = Math.max(((VLongWritable) value.get()[0]).get(), max); count =
	 * count + ((VLongWritable) value.get()[1]).get(); } max_count[0] = new
	 * VLongWritable(max); max_count[1] = new VLongWritable(count);
	 * 
	 * outValue2.set(max_count); context.write(key, outValue2); } else { for
	 * (VLongArrayWritable value : values) { context.write(key, value); } } } }
	 * 
	 * public static class semiBlockListCounterReducer extends
	 * Reducer<VLongWritable, VLongArrayWritable, VLongWritable,
	 * VLongArrayWritable> { // input (0 max count) || (src dst1 dst2 .. dstx)
	 * // output (0 max count/2 0) || (src dst1 dst2 .. dstx) public void
	 * reduce(VLongWritable key, Iterable<VLongArrayWritable> values, Context
	 * context) throws IOException, InterruptedException { if (key.get() == 0) {
	 * VLongArrayWritable outValue2 = new VLongArrayWritable(); VLongWritable[]
	 * max_count = new VLongWritable[4];
	 * 
	 * long max = 0; long count = 0; for (VLongArrayWritable value : values) {
	 * max = Math.max(((VLongWritable) value.get()[0]).get(), max); count =
	 * count + ((VLongWritable) value.get()[1]).get(); } max_count[0] = new
	 * VLongWritable(max); max_count[1] = new VLongWritable((count / 2));
	 * max_count[2] = new VLongWritable(111); max_count[3] = new
	 * VLongWritable(1);
	 * 
	 * outValue2.set(max_count); context.write(key, outValue2); } else { for
	 * (VLongArrayWritable value : values) { context.write(key, value); } } } }
	 */
	public static void main(String[] args) throws Exception {

		boolean stageA = false, stageB = false, stageC = false, stageD = false;
		
		if(args.length!=3){
			System.err.println("Argument count error. Class argument format: [jobID] [inputPath] [outputPath]");
			System.exit(-1);
		}

		int jobID = Integer.parseInt(args[0]);

		Configuration confA = new Configuration();
		confA.setQuietMode(true);

		confA.set("PARTID_DISPLACEMENT", 1 + "");
		confA.set("EDGE_WEIGHT", 0 + "");
		Job jobA = new Job(confA, args[0] + "- semiBlockSummery");
		jobA.setOutputFormatClass(LongArrayWriter.class);
		jobA.setJarByClass(semiBlockPartitioning_hadoop.class);
		jobA.setMapperClass(semiBlockSummeryMapper.class);
		jobA.setCombinerClass(semiBlockSummeryCombiner.class);
		jobA.setReducerClass(semiBlockSummeryReduce.class);

		jobA.setOutputKeyClass(IntWritable.class);
		jobA.setOutputValueClass(VLongArrayWritable.class);
		FileInputFormat.addInputPath(jobA, new Path(args[1]));
		FileOutputFormat.setOutputPath(jobA, new Path(args[2] + args[0]));
		stageA = jobA.waitForCompletion(false);

		if (stageA) {
			Configuration confB = new Configuration();
			confB.setQuietMode(true);

			jobID++;

			Job jobB = new Job(confB, jobID + "- semiBlockMetisFormat");
			jobB.setOutputFormatClass(LongArrayWriter.class);
			jobB.setJarByClass(semiBlockPartitioning_hadoop.class);
			jobB.setMapperClass(semiBlockMetisFormatMapper.class);
			jobB.setCombinerClass(semiBlockMetisFormatCombiner.class);
			jobB.setReducerClass(semiBlockMetisFormatReducer.class);

			jobB.setOutputKeyClass(IntWritable.class);
			jobB.setOutputValueClass(VLongArrayWritable.class);
			FileInputFormat.addInputPath(jobB, new Path(args[2] + (jobID - 1)));
			FileOutputFormat.setOutputPath(jobB, new Path(args[2] + jobID));

			stageB = jobB.waitForCompletion(false);
		}

		long maxVertex = 0;
		if (stageA && stageB) {
			Configuration confC = new Configuration();
			confC.setQuietMode(true);
			confC.set("EDGE_WEIGHT", 1 + "");

			jobID++;

			Job jobC = new Job(confC, jobID + "- semiBlockListCounter");
			jobC.setOutputFormatClass(LongArrayWriter.class);
			jobC.setJarByClass(preMetisCountAppend_hadoop.class);
			jobC.setMapperClass(listCounterMapper.class);
			jobC.setCombinerClass(listCounterCombiner.class);
			jobC.setReducerClass(listCounterReducer.class);
			jobC.setOutputFormatClass(LongArrayWriter.class);

			jobC.setOutputKeyClass(VLongWritable.class);
			jobC.setOutputValueClass(VLongArrayWritable.class);
			FileInputFormat.addInputPath(jobC, new Path(args[2] + (jobID - 1)));
			FileOutputFormat.setOutputPath(jobC, new Path(args[2] + jobID));

			stageC = jobC.waitForCompletion(false);

			maxVertex = jobC.getCounters().findCounter(
							preMetisCountAppend_hadoop.mapRedMessage.MAX_VERTEX).getValue();
		}
		if (stageA && stageB && stageC) {
			Configuration confD = new Configuration();
			confD.setQuietMode(true);
			confD.set("MAX_VERTEX", maxVertex + "");

			jobID++;

			Job jobD = new Job(confD, jobID + "- preMFixEmptyVertex");
			jobD.setOutputFormatClass(LongArrayWriter.class);
			jobD.setJarByClass(preMetisCountAppend_hadoop.class);
			jobD.setMapperClass(FixEmptyVertexMapper.class);
			jobD.setCombinerClass(FixEmptyVertexCombiner.class);
			jobD.setReducerClass(FixEmptyVertexCombiner.class);
			jobD.setOutputFormatClass(noKeyLongWriter.class);
			jobD.setNumReduceTasks(1);
			jobD.setOutputKeyClass(VLongWritable.class);
			jobD.setOutputValueClass(VLongArrayWritable.class);
			FileInputFormat.addInputPath(jobD, new Path(args[2] + (jobID - 1)));
			FileOutputFormat.setOutputPath(jobD, new Path(args[2] + jobID));

			stageD = jobD.waitForCompletion(false);
		}

		System.exit((stageA && stageB && stageC && stageD) ? 0 : 1);
	}
}
