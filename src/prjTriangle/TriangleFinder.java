package prjTriangle;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import prjTriangle.LongWritableTriplet.TripleComparator;
import prjTriangle.LongWritableTriplet.TripleGroupingComparator;
import prjTriangle.TriangleFinderMapper;
import prjTriangle.TriangleFinderReducer;

//hadoop jar Triangle.jar prjTriangle.TriangleFinder 1 INPUT/twitter/twitter-small.txt OUTPUT/twitter

public class TriangleFinder extends Configured implements Tool {

	private Path outputDir;
	private Path inputPath;
	private int numReducers;

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();

		Job job = Job.getInstance(conf, "TriangleFinder");
		job.setJarByClass(TriangleFinder.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TriangleFinderMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritablePair.class);

		job.setReducerClass(TriangleFinderReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setNumReduceTasks(this.numReducers);
		job.setOutputFormatClass(TextOutputFormat.class);
		Path in = this.inputPath;
		FileInputFormat.addInputPath(job, in);
		Path out = new Path(this.outputDir.toString() + "_partial");
		FileSystem dfs = FileSystem.get(getConf());
		if (dfs.exists(out))
			dfs.delete(out, true);

		// SequenceFileOutputFormat.setOutputPath(job, out);
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job, out);

		job.waitForCompletion(true);

		Job job2 = Job.getInstance(conf, "TriangleFinder2");
		job2.setJarByClass(TriangleFinder.class);

		job2.setInputFormatClass(TextInputFormat.class);

		job2.setMapperClass(TriangleFinderMapper2.class);
		job2.setMapOutputKeyClass(LongWritableTriplet.class);
		job2.setMapOutputValueClass(LongWritable.class);
		// job2.setPartitionerClass(TriangleFinderPartitioner.class);
		job2.setReducerClass(TriangleFinderReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setSortComparatorClass(TripleComparator.class);
		job2.setGroupingComparatorClass(TripleGroupingComparator.class);

		job2.setOutputValueClass(Text.class);
		job2.setNumReduceTasks(this.numReducers);
		job2.setOutputFormatClass(TextOutputFormat.class);
		Path in2 = new Path(out.toString() + "/part-r-00000");
		FileInputFormat.addInputPath(job2, in2);
		FileInputFormat.addInputPath(job2, in);

		// SequenceFileInputFormat.addInputPath(job2, in2);

		if (dfs.exists(this.outputDir))
			dfs.delete(this.outputDir, true);
		FileOutputFormat.setOutputPath(job2, this.outputDir);

		job2.waitForCompletion(true);

		return 1;

	}

	public TriangleFinder(String[] args) {
		if (args.length != 3) {
			this.numReducers = 1;
			this.inputPath = new Path(
					"/home/student/INPUT/twitter/twitter-verysmall.txt");
			this.outputDir = new Path("/home/student/OUTPUT/twitter");
		} else {
			this.numReducers = Integer.parseInt(args[0]);
			this.inputPath = new Path(args[1]);
			this.outputDir = new Path(args[2]);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TriangleFinder(args),
				args);
		System.exit(res);
	}

}
