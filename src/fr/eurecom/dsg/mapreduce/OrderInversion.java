package fr.eurecom.dsg.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class OrderInversion extends Configured implements Tool {

	private final static String ASTERISK = "\0";

	public static class PartitionerTextPair extends
			Partitioner<TextPair, IntWritable> {
		@Override
		public int getPartition(TextPair key, IntWritable value,
				int numReduceTasks) {
			return (toUnsigned(key.getFirst().hashCode()) % numReduceTasks);
		}

		/**
		 * toUnsigned(10) = 10 toUnsigned(-1) = 2147483647
		 * 
		 * @param val
		 *            Value to convert
		 * @return the unsigned number with the same bits of val
		 * */
		public static int toUnsigned(int val) {
			return val & Integer.MAX_VALUE;
		}
	}

	public static class PairMapper extends
			Mapper<LongWritable, Text, TextPair, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws java.io.IOException, InterruptedException {

			String line = value.toString();
			line = line.replaceAll("[^a-zA-Z0-9_]+", " ");
			line = line.replaceAll("^\\s+", "");
			String[] tokens = line.split("\\s+");
			for (int i = 0; i < tokens.length - 1; i++) {
				for (int j = Math.max(0, i - 1); j < Math.min(tokens.length,
						i + 2); j++) {
					if (i == j)
						continue;
					context.write(new TextPair(tokens[i], tokens[j]), new IntWritable(1));
					context.write(new TextPair(tokens[i], ASTERISK), new IntWritable(1));
				}
			}
		}
	}

	public static class PairReducer extends	Reducer<TextPair, IntWritable, TextPair, DoubleWritable> {

		private int cooccurrence_count;
		private int s;

		@Override
		public void reduce(TextPair key, Iterable<IntWritable> values,	Context context) throws IOException,InterruptedException {
			if (key.getSecond().compareTo(new Text(ASTERISK)) == 0) {
				cooccurrence_count = 0;
				for (IntWritable value : values) {
					cooccurrence_count += value.get();
				}
			} else {
				s = 0;
				for (IntWritable value : values) {
					s += value.get();
				}
				try {
					context.write(key, new DoubleWritable((double)cooccurrence_count));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	private int numReducers;
	private Path inputPath;
	private Path outputDir;

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {

			System.err.printf("%s requires two arguments\n", getClass()
					.getSimpleName());

			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		int numreducer = Integer.parseInt(args[2]);

		Configuration conf = getConf();
		Job job = new Job(conf, "Pair Relative");

		job.setJarByClass(Pair.class);

		job.setMapperClass(PairMapper.class);
		job.setReducerClass(PairReducer.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(DoubleWritable.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setPartitionerClass(PartitionerTextPair.class);
		// job.setGroupingComparatorClass(GroupComparator.class);
		job.setSortComparatorClass(TextPair.Comparator.class);

		job.setNumReduceTasks(numreducer);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	OrderInversion(String[] args) {
		if (args.length != 3) {
			this.numReducers = 2;
			this.inputPath = new Path("/home/student/INPUT/text/quote.txt");
			this.outputDir = new Path("/home/student/OUTPUT/wordcountPair/");
		} else {
			this.numReducers = Integer.parseInt(args[0]);
			this.inputPath = new Path(args[1]);
			this.outputDir = new Path(args[2]);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new OrderInversion(args),
				args);
		System.exit(res);
	}
}
