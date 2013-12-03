package postPartition;

import modClasses.LongArrayWritable;
import modClasses.LongArrayWriter;
import modClasses.VLongArrayWritable;
import modClasses.mapKeyComparatorBinLong;
import modClasses.mapKeyComparatorInt;
import modClasses.mapKeyPartitionerBinLongLongArray;
import modClasses.mapKeyPartitionerIntVLongArray;
import modClasses.noKeyLongWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import postPartition.groupSubGraph_hadoop.groupSubGMapper;
import postPartition.postMetisMatchVertexPartition_hadoop.matchVertexMapperA;
import postPartition.postMetisMatchVertexPartition_hadoop.matchVertexMapperB;
import postPartition.postMetisMatchVertexPartition_hadoop.matchVertexReducerA;
import postPartition.postMetisMatchVertexPartition_hadoop.matchVertexReducerB;
import postPartition.postMetisVertexAssignID_hadoop.subGraphAssignMapper;
import postPartition.postMetisVertexAssignID_hadoop.subGraphAssignReducer;

public class semiBlockPostPartitioning_hadoop {

	public static void main(String[] args) throws Exception {

		boolean stageA = false, stageB = false, stageC = false, stageD = false;

		if (args.length != 7) {
			System.err
					.println("Argument count error. Class argument format: [jobID] [Partition Size] [original Partitions] [inputPath] [outputPath] [Final OutputPath] [duplicate outdegree]");
			System.exit(-1);
		}

		int jobID = Integer.parseInt(args[0]);

		Configuration confA = new Configuration();
		confA.setQuietMode(true);
		Job jobA = new Job(confA, jobID + "- postMVertexAssignID");

		jobA.setPartitionerClass(mapKeyPartitionerIntVLongArray.class);
		jobA.setGroupingComparatorClass(mapKeyComparatorInt.class);
		jobA.setSortComparatorClass(IntWritable.Comparator.class);
		jobA.setJarByClass(postMetisVertexAssignID_hadoop.class);
		jobA.setMapperClass(subGraphAssignMapper.class);
		jobA.setReducerClass(subGraphAssignReducer.class);
		jobA.setInputFormatClass(TextInputFormat.class);
		jobA.setOutputFormatClass(LongArrayWriter.class);
		jobA.setOutputKeyClass(IntWritable.class);
		jobA.setOutputValueClass(VLongArrayWritable.class);
		FileInputFormat.addInputPath(jobA, new Path(args[3]));
		FileOutputFormat.setOutputPath(jobA, new Path(args[4] + jobID));

		stageA = jobA.waitForCompletion(false);

		if (stageA) {
			jobID++;

			Configuration confB = new Configuration();
			confB.setQuietMode(true);
			confB.set("PARTID_DISPLACEMENT", 1 + "");
			confB.set("EDGE_WEIGHT", 0 + "");
			confB.set("GRAPH_PARTITIONED", "true");

			Job jobB = new Job(confB, jobID + "a- postMetisVertexMatch");

			jobB.setOutputFormatClass(noKeyLongWriter.class);

			jobB.setPartitionerClass(mapKeyPartitionerBinLongLongArray.class);
			jobB.setGroupingComparatorClass(mapKeyComparatorBinLong.class);

			jobB.setJarByClass(postMetisMatchVertexPartition_hadoop.class);
			jobB.setMapperClass(matchVertexMapperA.class);
			jobB.setReducerClass(matchVertexReducerA.class);

			jobB.setOutputKeyClass(LongWritable.class);
			jobB.setOutputValueClass(LongArrayWritable.class);
			FileInputFormat.addInputPaths(jobB, args[2] + "," + args[4]
					+ (jobID - 1));
			FileOutputFormat.setOutputPath(jobB, new Path(args[4] + jobID
					+ "_a"));
			stageB = jobB.waitForCompletion(false);
		}

		if (stageA && stageB) {
			// Starting Part B
			Configuration confC = new Configuration();
			confC.setQuietMode(true);
			confC.set("PARTID_DISPLACEMENT", 1 + "");
			confC.set("EDGE_WEIGHT", 0 + "");
			confC.set("GRAPH_PARTITIONED", "true");

			Job jobC = new Job(confC, jobID + "b- postMetisVertexMatch");

			jobC.setOutputFormatClass(noKeyLongWriter.class);

			jobC.setPartitionerClass(mapKeyPartitionerBinLongLongArray.class);
			jobC.setGroupingComparatorClass(mapKeyComparatorBinLong.class);
			jobC.setSortComparatorClass(LongWritable.Comparator.class);

			jobC.setJarByClass(postMetisMatchVertexPartition_hadoop.class);
			jobC.setMapperClass(matchVertexMapperB.class);
			jobC.setReducerClass(matchVertexReducerB.class);

			jobC.setOutputKeyClass(LongWritable.class);
			jobC.setOutputValueClass(LongArrayWritable.class);
			FileInputFormat.addInputPaths(jobC, args[4] + (jobID - 1) + ","
					+ args[4] + jobID + "_a");
			FileOutputFormat.setOutputPath(jobC, new Path(args[4] + jobID
					+ "_b"));
			stageC = jobC.waitForCompletion(false);
		}

		if (stageA && stageB && stageC) {
			jobID++;

			Configuration confD = new Configuration();
			confD.setQuietMode(true);

			confD.set("DUPLICATE_OUTEDGES", args[6] + "");

			Job jobD = new Job(confD, jobID + "- groupSubGraph");

			jobD.setOutputFormatClass(noKeyLongWriter.class);

			jobD.setJarByClass(groupSubGraph_hadoop.class);
			jobD.setMapperClass(groupSubGMapper.class);
			// job.setReducerClass(groupSubGReducer.class);

			jobD.setOutputKeyClass(IntWritable.class);
			jobD.setOutputValueClass(VLongArrayWritable.class);
			FileInputFormat.addInputPath(jobD, new Path(args[4] + (jobID - 1)
					+ "_b"));
			FileOutputFormat.setOutputPath(jobD, new Path(args[5]));
			jobD.setNumReduceTasks(Integer.parseInt(args[1]));
			stageD = jobD.waitForCompletion(false);
		}

		System.exit((stageA && stageB && stageC && stageD) ? 0 : 1);
	}
}
