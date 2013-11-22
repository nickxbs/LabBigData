package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceSideJoin extends Configured implements Tool {

	private Path outputDir;
	private Path inputPath;
	private int numReducers;

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();

		Job job = new Job(conf, "Word Count");

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(ReduceSideJoinMapper.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(ReduceSideJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, this.inputPath);
		FileOutputFormat.setOutputPath(job, this.outputDir);

		job.setNumReduceTasks(this.numReducers);

		job.setJarByClass(ReduceSideJoin.class);

		job.waitForCompletion(true);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public ReduceSideJoin(String[] args) {
		if (args.length != 3) {
			this.numReducers = 2;
			this.inputPath = new Path(
					"/home/student/INPUT/twitter/twitter-small.txt");
			this.outputDir = new Path("/home/student/OUTPUT/ReduceSideJoin"
					+ System.nanoTime());
		} else {
			this.numReducers = Integer.parseInt(args[0]);
			this.inputPath = new Path(args[1]);
			this.outputDir = new Path(args[2]);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ReduceSideJoin(args),
				args);
		System.exit(res);
	}

}

class ReduceSideJoinMapper extends
		Mapper<LongWritable, Text, TextPair, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		line = line.replaceAll("^\\s+", "");
		String[] sp = line.split("\\s+");// splits on TAB

		context.write(new TextPair(sp[1], "0"),
				new IntWritable(Integer.parseInt(sp[0])));// followed
		context.write(new TextPair(sp[0], "1"),
				new IntWritable(Integer.parseInt(sp[1])));// follower
	}
}

class ReduceSideJoinReducer extends
		Reducer<TextPair, IntWritable, Text, IntWritable> {
	protected List<Integer> follower = new ArrayList<Integer>();

	@Override
	protected void reduce(TextPair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		// List<Integer> follower = new ArrayList<Integer>();
		if (key.getSecond().toString().equals("1") && !follower.isEmpty()) {

			for (IntWritable value : values) {
				for (Integer f : follower)
					context.write(new Text(f.toString()),new IntWritable(value.get()));
			}
		} else {
			follower.clear();
			for (IntWritable value : values) {
				follower.add(value.get());
			}
		}
	}

}
