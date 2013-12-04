package fr.eurecom.dsg.mapreduce.join;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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




