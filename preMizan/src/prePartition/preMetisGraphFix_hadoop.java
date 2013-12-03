package prePartition;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.StringTokenizer;

import modClasses.LongArrayWriter;
import modClasses.VLongArrayWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Input graph as edge list (src dst)
//output graph as adjacency list (src count dst1 dst2 dst3.. dstx)

public class preMetisGraphFix_hadoop {

	public static class graphFixMapper extends
			Mapper<Object, Text, VLongWritable, VLongArrayWritable> {
		// input: (src dst)
		// output: (src dst) & (dst src)
		static long disp = 1;

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String input = value.toString();

			if (!input.startsWith("#") && input.trim().length() != 0) {
				StringTokenizer st = new StringTokenizer(input, " \t<>.");
				// Assume input format: (src dst)

				// Set original (src dst)
				VLongWritable src = new VLongWritable();

				VLongArrayWritable dst_array = new VLongArrayWritable();
				VLongWritable[] dst = new VLongWritable[1];
				dst[0] = new VLongWritable();

				src.set((Long.parseLong(st.nextToken()) + disp));
				dst[0].set((Long.parseLong(st.nextToken()) + disp));
				dst_array.set(dst);

				context.write(src, dst_array);

				// Set copy (dst src) for undirected graphs
				VLongWritable src2 = new VLongWritable(dst[0].get());

				VLongWritable[] dst2 = new VLongWritable[1];
				dst2[0] = new VLongWritable(src.get());
				VLongArrayWritable dst_array2 = new VLongArrayWritable();
				dst_array2.set(dst2);

				context.write(src2, dst_array2);

			}
		}
	}

	public static class graphFixCombiner
			extends
			Reducer<VLongWritable, VLongArrayWritable, VLongWritable, VLongArrayWritable> {
		// input (src dst)
		// output (src dst1 dst2 .. dstx)
		public void reduce(VLongWritable key,
				Iterable<VLongArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			/*
			 * ArrayList<Long> edge_set = new ArrayList<Long>();
			 * LongArrayWritable edge_list = new LongArrayWritable();
			 * 
			 * for(LongArrayWritable dst:values){
			 * //System.out.println(key.get()+
			 * " --> "+((VLongWritable)(dst.get())[0]).get());
			 * edge_set.add(((VLongWritable)(dst.get())[0]).get()); }
			 * 
			 * //output value format (dst1 dst2 .. dstx) VLongWritable [] edges
			 * = new VLongWritable[edge_set.size()]; int i=0; for (Long
			 * dst:edge_set){ edges[i] = new VLongWritable(dst.longValue());
			 * i++; } edge_list.set(edges);
			 * 
			 * context.write(key, edge_list);
			 */

			HashSet<Long> edge_set = new HashSet<Long>();

			VLongArrayWritable edge_list = new VLongArrayWritable();

			for (VLongArrayWritable dst : values) {
				for (int i = 0; i < dst.get().length; i++) {
					long abc = ((VLongWritable) (dst.get())[i]).get();
					edge_set.add(abc);
				}
			}
			// output value format (dst1 dst2 .. dstx)
			VLongWritable[] edges = new VLongWritable[edge_set.size()];
			int i = 0;
			for (Long dst : edge_set) {
				edges[i] = new VLongWritable(dst.longValue());
				i++;
			}
			edge_list.set(edges);
			context.write(key, edge_list);
		}
	}

	public static class graphFixReducer
			extends
			Reducer<VLongWritable, VLongArrayWritable, VLongWritable, VLongArrayWritable> {
		// input (src dst1 dst2 .. dstx)
		// output (src dst1 dst2 .. dstx)
		public void reduce(VLongWritable key,
				Iterable<VLongArrayWritable> values, Context context)
				throws IOException, InterruptedException {

			// System.out.println("My memory = "+Runtime.getRuntime().maxMemory());
			HashSet<VLongWritable> edge_set = new HashSet<VLongWritable>();
			// ArrayList<Long> edge_set = new ArrayList<Long>();
			try {

				VLongArrayWritable edge_list = new VLongArrayWritable();

				long time1 = new Date().getTime();
				for (VLongArrayWritable dst : values) {
					for (int i = 0; i < dst.get().length; i++) {
						// long abc = ((VLongWritable)(dst.get())[i]).get();
						edge_set.add(((VLongWritable) (dst.get())[i]));
					}
				}
				long time2 = new Date().getTime();
				// System.out.println("Time to load = "+(time2-time1));

				// output value format (count dst1 dst2 .. dstx)
				// OPT1
				VLongWritable[] edges = new VLongWritable[edge_set.size()];
				// LongWritable [] edges = new LongWritable[1];
				// edges["Farania Rangkuti" <farania.rangkuti@kaust.edu.sa>, 0] = new VLongWritable(edge_set.size());
				// edges[0] = new LongWritable(fuck);

				// long time3 = new Date().getTime();
				int i = 0;
				for (VLongWritable dst : edge_set) {
					edges[i] = dst;// new LongWritable(dst.longValue());
					i++;
				}
				// //long time4 = new Date().getTime();
				// System.out.println("Time to copy = "+(time4-time3));

				// long time5 = new Date().getTime();
				edge_list.set(edges);
				// long time6 = new Date().getTime();
				// System.out.println("Time to set = "+(time6-time5));

				// long time7 = new Date().getTime();
				context.write(key, edge_list);
				// long time8 = new Date().getTime();
				// System.out.println("Time to Write = "+(time8-time7));
			} catch (Exception e) {
				System.out.println("Something wrong with key: " + key.get());
				// System.out.println("hashset size = "+edge_set.size());
				System.out.println("************** StackTrace **************");
				e.printStackTrace();
				System.out.println("****************************************");

			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setQuietMode(true);

		Job job = new Job(conf, "1- preMGraphFix");

		job.setJarByClass(preMetisGraphFix_hadoop.class);
		job.setMapperClass(graphFixMapper.class);
		// job.setCombinerClass(graphFixCombiner.class);
		job.setReducerClass(graphFixReducer.class);
		job.setOutputFormatClass(LongArrayWriter.class);

		job.setOutputKeyClass(VLongWritable.class);
		job.setOutputValueClass(VLongArrayWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// job.setNumReduceTasks(5);
		// submit and wait
		System.exit(job.waitForCompletion(false) ? 0 : 1);
	}
}
