package partitioning;

import java.io.IOException;
import java.util.StringTokenizer;

import modClasses.extraKeyTextWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//input graph in format (src [weight] dst)
//output (vertex+1 partID)

public class modHashPartitioning_hadoop {

	//input graph in format (src [weight] dst)
	//output ((src+disp) partID) && ((dst+disp) partID) ==> partID = vertexID % totalParts
	
	public static class modHashMapper extends
			Mapper<Object, Text, VLongWritable, IntWritable> {
		
		long disp;  
		int totalParts;
		int edgeWeight;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.totalParts = Integer.parseInt(context.getConfiguration().get(
					"TOTAL_PARTS"));
			this.disp = Long.parseLong(context.getConfiguration().get(
			"VERTEX_DISPLACEMENT"));
			this.edgeWeight = Integer.parseInt(context.getConfiguration().get(
			"EDGE_WEIGHT"));
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String input = value.toString();
			if (!input.startsWith("#") && input.trim().length() != 0) {
				StringTokenizer st = new StringTokenizer(input, " \t");
				long srcV = Long.parseLong(st.nextToken());
				//Ignores edge weights
				for (int i=0;i<edgeWeight;i++){
					st.nextToken();
				}
				long dstV = Long.parseLong(st.nextToken());

				int srcSubG = (int) (srcV % totalParts);
				int dstSubG = (int) (dstV % totalParts);

				VLongWritable key1 = new VLongWritable(srcV+disp);
				VLongWritable key2 = new VLongWritable(dstV+disp);

				IntWritable val1 = new IntWritable(srcSubG);
				IntWritable val2 = new IntWritable(dstSubG);

				context.write(key1, val1);
				context.write(key2, val2);
			}
		}
	}

	//input (vertex partID)
	//output (vertex partID) ==> single copy
	public static class modHashCombiner extends
			Reducer<VLongWritable, IntWritable, VLongWritable, IntWritable> {
		public void reduce(VLongWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			IntWritable outValue = values.iterator().next();
			context.write(key, outValue);
		}
	}

	//input (vertex partID)
	//output (vertex partID) ==> single copy
	public static class modHashReducer extends
			Reducer<VLongWritable, IntWritable, VLongWritable, IntWritable> {
		
		public void reduce(VLongWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			IntWritable outValue = values.iterator().next();
			VLongWritable key2 = new VLongWritable(key.get());
			context.write(key2, outValue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setQuietMode(true);

		conf.set("TOTAL_PARTS", args[0] + "");
		conf.set("VERTEX_DISPLACEMENT", 1+"");
		conf.set("EDGE_WEIGHT", args[3]);

		Job job = new Job(conf, "modHashMapper");
		job.setOutputFormatClass(extraKeyTextWriter.class);
		job.setJarByClass(modHashPartitioning_hadoop.class);
		job.setMapperClass(modHashMapper.class);
		job.setCombinerClass(modHashReducer.class);
		job.setReducerClass(modHashReducer.class);

		job.setOutputKeyClass(VLongWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		// job.setNumReduceTasks(Integer.parseInt(args[0]));
		// submit and wait
		System.exit(job.waitForCompletion(false) ? 0 : 1);
	}
}
