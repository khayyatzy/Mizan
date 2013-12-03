package prePartition;

import java.io.IOException;
import java.util.StringTokenizer;

import modClasses.VLongArrayWritable;
import modClasses.LongArrayWriter;
import modClasses.noKeyLongWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class preMetisCountAppend_hadoop {

	// ############## listCounterJob ##############
	public static enum mapRedMessage {
		MAX_VERTEX, CUR_VERTEX
	}

	// Input graph as adjacency list (src dst1 dst2 .. dstx)
	// output graph as adjacency list (src dst1 dst2 .. dstx) with (0 max count
	// 0)
	public static class listCounterMapper extends
			Mapper<Object, Text, VLongWritable, VLongArrayWritable> {
		// input: (src count dst1 dst2 .. dstx)
		// output: (src dst1 dst2 .. dstx) & (0 max count)
		int edgeWeight;
		
		

		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.edgeWeight = Integer.parseInt(context.getConfiguration().get(
					"EDGE_WEIGHT"));
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String input = value.toString();
			StringTokenizer st = new StringTokenizer(input, " \t");

			// value = (dst1 dst2 .. dstx)
			int totalCount = st.countTokens();
			VLongArrayWritable outValue1 = new VLongArrayWritable();
			VLongWritable[] adjList = new VLongWritable[(totalCount - 1)];

			// value = (max count)
			VLongArrayWritable outValue2 = new VLongArrayWritable();
			VLongWritable[] max_count = new VLongWritable[2];

			// read the key from file
			VLongWritable outKey1 = new VLongWritable(Long.parseLong(st
					.nextToken()));
			VLongWritable outKey2 = new VLongWritable(0);

			// read (src)
			// OPT1
			// ==> just from my mind ;p
			//System.out.println("totalCount = " + totalCount + " or "
//					/+ (totalCount - (edgeWeight + 1) / (edgeWeight + 1)));
			max_count[1] = new VLongWritable((totalCount - (edgeWeight + 1)
					/ (edgeWeight + 1)));

			// Assume the src is the maximum
			long max = outKey1.get();

			int j = 1;
			for (int i = 0; i < adjList.length; i++) {
				adjList[i] = new VLongWritable(Long.parseLong(st.nextToken()));
				if (edgeWeight == 0) {
					max = Math.max(max, adjList[i].get());
				} else if ((j % (edgeWeight + 1)) == 0) {
					// select the nth element which is suppose to be vertex not
					// weight(s)
					max = Math.max(max, adjList[i].get());
				}
				j++;
			}
			max_count[0] = new VLongWritable(max);

			outValue1.set(adjList);
			outValue2.set(max_count);

			// output: (src dst1 dst2 .. dstx)
			context.write(outKey1, outValue1);

			// output: (0 max count)
			context.write(outKey2, outValue2);
		}
	}

	public static class listCounterCombiner
			extends
			Reducer<VLongWritable, VLongArrayWritable, VLongWritable, VLongArrayWritable> {
		// input (0 max count) || (src dst1 dst2 .. dstx)
		// output (0 max count) || (src dst1 dst2 .. dstx)

		public void reduce(VLongWritable key,
				Iterable<VLongArrayWritable> values, Context context)
				throws IOException, InterruptedException {

			if (key.get() == 0) {
				VLongArrayWritable outValue2 = new VLongArrayWritable();
				VLongWritable[] max_count = new VLongWritable[2];

				long max = 0;
				long count = 0;
				for (VLongArrayWritable value : values) {
					max = Math.max(((VLongWritable) value.get()[0]).get(), max);
					count = count + ((VLongWritable) value.get()[1]).get();
				}
				max_count[0] = new VLongWritable(max);
				max_count[1] = new VLongWritable(count);

				outValue2.set(max_count);
				context.write(key, outValue2);
			} else {
				for (VLongArrayWritable value : values) {
					context.write(key, value);
				}
			}
		}
	}

	public static class listCounterReducer
			extends
			Reducer<VLongWritable, VLongArrayWritable, VLongWritable, VLongArrayWritable> {
		// input (0 max count) || (src dst1 dst2 .. dstx)
		// output (0 max count/2 0) || (src dst1 dst2 .. dstx)
		int edgeWeight;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.edgeWeight = Integer.parseInt(context.getConfiguration().get(
					"EDGE_WEIGHT"));
		}

		public void reduce(VLongWritable key,
				Iterable<VLongArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			if (key.get() == 0) {
				VLongArrayWritable outValue2 = new VLongArrayWritable();

				VLongWritable[] max_count;
				if (edgeWeight == 0) {
					// Assume no edgeWeight no vertex weight
					max_count = new VLongWritable[3];
				} else {
					// edgeWeight and vertex weight
					max_count = new VLongWritable[4];
				}

				long max = 0;
				long count = 0;
				for (VLongArrayWritable value : values) {
					max = Math.max(((VLongWritable) value.get()[0]).get(), max);
					count = count + ((VLongWritable) value.get()[1]).get();
				}
				max_count[0] = new VLongWritable(max);
				max_count[1] = new VLongWritable((count / 2));
				if (edgeWeight == 0) {
					max_count[2] = new VLongWritable(0);
				} else {
					max_count[2] = new VLongWritable(111);
					max_count[3] = new VLongWritable(edgeWeight);
				}

				outValue2.set(max_count);
				context.write(key, outValue2);

				context.getCounter(mapRedMessage.MAX_VERTEX).increment(max);

			} else {
				context.getCounter(mapRedMessage.CUR_VERTEX).increment(1);
				for (VLongArrayWritable value : values) {
					context.write(key, value);
				}
			}
		}
	}

	// ############## FixEmptyVertexJob ##############
	// input graph as adjacency list (src dst1 dst2 .. dstx) with (0 max count
	// 0)
	// output graph as adjacency list (dst1 dst2 .. dstx) with (max count 0)
	// including empty lines representing empty vertexes
	public static class FixEmptyVertexMapper extends
			Mapper<Object, Text, VLongWritable, VLongArrayWritable> {
		// input: (src dst1 dst2 .. dstx) || (0 max count 0)
		// output: (src dst1 dst2 .. dstx) && (src_i -- src_j) || (0 max count
		// 0)
		long maxCount = 0;
		// long mapChunck = 0;
		// long mapCount = 0;
		boolean broadCast = false;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.maxCount = Long.parseLong(context.getConfiguration().get(
					"MAX_VERTEX"));
			// this.mapChunck =
			// Long.parseLong(context.getConfiguration().get("MAP_CHUNCK"));
			// this.mapCount =
			// Long.parseLong(context.getConfiguration().get("MAP_COUNT"));
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			if (!broadCast) {
				long mapCount = Long.parseLong(context.getConfiguration().get(
						"mapred.map.tasks"));
				long mapChunck = maxCount / mapCount;
				long myID = Long.parseLong(context.getTaskAttemptID()
						.toString().split("_")[4]);
				//System.out.println("I am " + myID + " out of " + mapCount);
				this.broadCast = true;

				long start = myID * mapChunck + 1;
				long stop = (myID + 1) * mapChunck + 1;

				if (mapCount == (myID + 1)) {
					stop = this.maxCount + 1;
				}

				// System.out.println("context.getInputSplit().getLocations().length = "+
				// context.getInputSplit().getLocations().length +
				// " start = "+start+" stop = "+
				// stop+" maxCount = "+maxCount+" mapChunck = "+mapChunck+" mapCount = "+mapCount);

				// Dummy val
				VLongWritable[] myValArray = new VLongWritable[0];
				VLongArrayWritable myVal = new VLongArrayWritable();
				myVal.set(myValArray);

				for (long i = start; i < stop; i = i + 1) {
					VLongWritable myKey = new VLongWritable(i);
					// System.out.println("Key "+i+" {empty}");
					context.write(myKey, myVal);
				}
			}

			String input = value.toString();
			StringTokenizer st = new StringTokenizer(input, " \t");

			VLongWritable[] outValArray = new VLongWritable[(st.countTokens() - 1)];
			VLongArrayWritable outVal = new VLongArrayWritable();

			VLongWritable outKey = new VLongWritable(Long.parseLong(st
					.nextToken()));
			int i = 0;
			while (st.hasMoreTokens()) {
				outValArray[i] = new VLongWritable(Long.parseLong(st
						.nextToken()));
				i++;
			}
			outVal.set(outValArray);
			// System.out.println("Key "+outKey.get()+" value "+outVal.toString());
			context.write(outKey, outVal);
		}
	}

	public static class FixEmptyVertexCombiner
			extends
			Reducer<VLongWritable, VLongArrayWritable, VLongWritable, VLongArrayWritable> {
		// input (0 max count) || (src dst1 dst2 .. dstx) || (src)
		// output (0 max count) || (src dst1 dst2 .. dstx) || (src)
		public void reduce(VLongWritable key,
				Iterable<VLongArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			int counter = 0;
			// VLongArrayWritable myVal = values.iterator().next();
			for (VLongArrayWritable inputArray : values) {
				if (inputArray.get().length != 0) {
					context.write(key, inputArray);
					counter++;
				}
			}
			if (counter == 0) {
				VLongArrayWritable dummy = new VLongArrayWritable();
				VLongWritable[] dummyArray = new VLongWritable[0];
				dummy.set(dummyArray);
				context.write(key, dummy);
			}
			/*
			 * if(values.iterator().hasNext()){ if(myVal.get().length==0){
			 * VLongArrayWritable myVal2 = values.iterator().next();
			 * context.write(key,myVal2); } else{ context.write(key,myVal); } }
			 * else{ context.write(key,myVal); }
			 */
		}
	}

	public static void main(String[] args) throws Exception {

		// ############## Stage1: MetisCountAppend ####################

		Configuration conf = new Configuration();
		conf.setQuietMode(true);
		conf.set("EDGE_WEIGHT", 0 + "");

		Job job = new Job(conf, "2- preMCountAppend");

		job.setJarByClass(preMetisCountAppend_hadoop.class);
		job.setMapperClass(listCounterMapper.class);
		job.setCombinerClass(listCounterCombiner.class);
		job.setReducerClass(listCounterReducer.class);

		job.setOutputKeyClass(VLongWritable.class);
		job.setOutputValueClass(VLongArrayWritable.class);
		job.setOutputFormatClass(LongArrayWriter.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// job.setNumReduceTasks(17);

		boolean stage1 = job.waitForCompletion(false);

		long maxVertex = job.getCounters()
				.findCounter(mapRedMessage.MAX_VERTEX).getValue();
		long currVertex = job.getCounters()
				.findCounter(mapRedMessage.CUR_VERTEX).getValue();
		// long mapCount =
		// job.getCounters().findCounter(mapRedMessage.MAP_COUNT).getValue();

		System.out.println("MaxVertex = " + maxVertex);
		System.out.println("CurrVertex = " + currVertex);
		// System.out.println("MapCount = "+mapCount);

		// ############## Stage2: FixEmptyVertex ####################

		Configuration conf2 = new Configuration();

		// Get the expected mapCount from input HDFS path
		/*
		 * FileSystem hdfs = FileSystem.get(conf2); Path inputPath = new
		 * Path(args[1]); FileStatus [] fst = hdfs.listStatus(inputPath); long
		 * calcMapCount = 0; double blockSize = 0; double fileSize = 0; for(int
		 * i=0;i<fst.length;i++){ if(!fst[i].isDir()){ blockSize =
		 * fst[i].getBlockSize(); fileSize = fst[i].getLen(); calcMapCount =
		 * calcMapCount + (long)(Math.ceil(fileSize/blockSize)); } }
		 * System.out.println
		 * ("blockSize = "+blockSize+" fileSize = "+fileSize+" calcMapCount = "
		 * +calcMapCount);
		 */

		// set configuration to next stage
		conf2.set("MAX_VERTEX", maxVertex + "");
		// conf2.set("MAP_CHUNCK", maxVertex/calcMapCount+"");
		// conf2.set("MAP_COUNT", ((long)calcMapCount)+"");
		conf2.setQuietMode(true);

		Job job2 = new Job(conf2, "3- preMFixEmptyVertex");

		job2.setJarByClass(preMetisCountAppend_hadoop.class);
		job2.setMapperClass(FixEmptyVertexMapper.class);
		job2.setCombinerClass(FixEmptyVertexCombiner.class);
		job2.setReducerClass(FixEmptyVertexCombiner.class);

		job2.setOutputFormatClass(noKeyLongWriter.class);
		job2.setOutputKeyClass(VLongWritable.class);
		job2.setOutputValueClass(VLongArrayWritable.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		// Set ouput to be a single file for metis
		job2.setNumReduceTasks(1);

		boolean stage2 = job2.waitForCompletion(false);

		System.exit((stage1 && stage2) ? 0 : 1);
	}
}
