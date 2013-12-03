package graphFormat;

import modClasses.LongArrayWriter;
import modClasses.LongArrayWriterJSON;
import modClasses.VLongArrayWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import prePartition.preMetisGraphFix_hadoop;
import prePartition.preMetisGraphFix_hadoop.graphFixMapper;
import prePartition.preMetisGraphFix_hadoop.graphFixReducer;

public class giraphJsonOutdegree {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setQuietMode(true);

		Job job = new Job(conf, "1- Giraph Graph Format");

		job.setJarByClass(giraphJsonOutdegree.class);
		job.setMapperClass(preMetisGraphFix_hadoop.graphFixMapper.class);
		// job.setCombinerClass(graphFixCombiner.class);
		job.setReducerClass(preMetisGraphFix_hadoop.graphFixReducer.class);
		job.setOutputFormatClass(LongArrayWriterJSON.class);

		job.setOutputKeyClass(VLongWritable.class);
		job.setOutputValueClass(VLongArrayWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(1);
		// submit and wait
		System.exit(job.waitForCompletion(false) ? 0 : 1);
	}
}
