package statistics;

import java.io.IOException;
import java.util.StringTokenizer;

import modClasses.LongArrayWriter;
import modClasses.VLongArrayWritable;
import modClasses.noKeyLongWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import postPartition.groupSubGraph_hadoop;
import postPartition.groupSubGraph_hadoop.groupSubGMapper;

public class partitionQuality_hadoop {

	public static class partitionQualityMapper extends
			Mapper<Object, Text, VIntWritable, VLongArrayWritable> {

		int edgeWeight;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.edgeWeight = Integer.parseInt(context.getConfiguration().get(
					"EDGE_WEIGHT"));
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			StringTokenizer st = new StringTokenizer(value.toString() + " \t");
			VLongArrayWritable outValue = new VLongArrayWritable();
			VLongWritable[] outValueArray = new VLongWritable[2];

			st.nextToken();
			VIntWritable outKey = new VIntWritable(Integer.parseInt(st
					.nextToken()));

			long weight = 1;
			for (int i = 0; i < edgeWeight; i++) {
				weight = weight + Long.parseLong(st.nextToken());
			}

			st.nextToken();
			int dstLocation = Integer.parseInt(st.nextToken());

			if (dstLocation == outKey.get()) {
				outValueArray[0] = new VLongWritable(weight);
				outValueArray[1] = new VLongWritable(0);
			} else {
				outValueArray[0] = new VLongWritable(0);
				outValueArray[1] = new VLongWritable(weight);
			}

			outValue.set(outValueArray);
			context.write(outKey, outValue);
		}
	}

	public static class partitionQualityReducer
			extends
			Reducer<VIntWritable, VLongArrayWritable, VIntWritable, VLongArrayWritable> {

		boolean duplicate;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.duplicate = Boolean.parseBoolean((context.getConfiguration()
					.get("DUPLICATE_OUTEDGES")));
		}

		public void reduce(VIntWritable key,
				Iterable<VLongArrayWritable> values, Context context)
				throws IOException, InterruptedException {

			long counter0 = 0;
			long counter1 = 0;

			for (VLongArrayWritable inValue : values) {
				counter0 = counter0 + ((VLongWritable) inValue.get()[0]).get();
				counter1 = counter1 + ((VLongWritable) inValue.get()[1]).get();
			}

			VLongArrayWritable outValue = new VLongArrayWritable();
			VLongWritable[] outValueArray = new VLongWritable[2];
			outValueArray[0] = new VLongWritable(counter0);
			outValueArray[1] = new VLongWritable(counter1);
			outValue.set(outValueArray);

			context.write(key, outValue);
		}
	}

	public static void main(String[] args) throws Exception {
		
		if (args.length != 4) {
			System.err
					.println("Argument count error. Class argument format: [inputPath] [outputPath] [edge weight] [duplicate outdegree]");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		conf.setQuietMode(true);

		conf.set("DUPLICATE_OUTEDGES", args[3]);
		conf.set("EDGE_WEIGHT", args[2]);
	
		Job job = new Job(conf, "partitionQuality");

		job.setOutputFormatClass(LongArrayWriter.class);

		job.setJarByClass(partitionQuality_hadoop.class);
		job.setMapperClass(partitionQualityMapper.class);
		job.setCombinerClass(partitionQualityReducer.class);
		job.setReducerClass(partitionQualityReducer.class);

		job.setOutputKeyClass(VIntWritable.class);
		job.setOutputValueClass(VLongArrayWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(1);
		// submit and wait
		System.exit(job.waitForCompletion(false) ? 0 : 1);
	}
}
