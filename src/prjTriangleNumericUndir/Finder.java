package prjTriangleNumericUndir;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//hadoop jar TriangleWiki.jar prjTriangleWiki.Finder 1 INPUT/ttter/twitter-big-sample.txt OUTPUT/twitterBig
//hadoop jar Triangle.jar prjTriangle.TriangleFinder 1 INPUT/twitter/twitter-small.txt OUTPUT/twitter

public class Finder extends Configured implements Tool {

	private Path outputDir;
	private Path inputPath;
	private int numReducers;

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();

		Job job = new Job(conf, "TriangleFinder");
		job.setJarByClass(Finder.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(Mapper1.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setSortComparatorClass(Comparator1.class);
		job.setGroupingComparatorClass(GroupingComparator1.class);
		job.setPartitionerClass(Partitioner1.class);

		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(LongLong.class);
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

		Job job2 = new Job(conf, "TriangleFinder2");
		job2.setJarByClass(Finder.class);

		job2.setInputFormatClass(TextInputFormat.class);

		job2.setMapperClass(Mapper2.class);
		job2.setMapOutputKeyClass(LongLongBit.class);
		job2.setMapOutputValueClass(LongWritable.class);
		// job2.setPartitionerClass(TriangleFinderPartitioner.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setSortComparatorClass(Comparator2.class);
		job2.setGroupingComparatorClass(GroupingComparator2.class);
		job2.setPartitionerClass(Partitioner2.class);

		job2.setNumReduceTasks(this.numReducers);
		job2.setOutputFormatClass(TextOutputFormat.class);
		for(Integer ix=0;ix<this.numReducers;ix++){
			Path in2 = new Path(out.toString() + "/part-r-0000"+ix);
			FileInputFormat.addInputPath(job2, in2);

		}
		FileInputFormat.addInputPath(job2, in);

		// SequenceFileInputFormat.addInputPath(job2, in2);

		if (dfs.exists(this.outputDir))
			dfs.delete(this.outputDir, true);
		FileOutputFormat.setOutputPath(job2, this.outputDir);

		job2.waitForCompletion(true);

		return 1;

	}

	public Finder(String[] args) {
		if (args.length != 3) {
			this.numReducers = 1;
			this.inputPath = new Path("INPUT/youtube/youtube.txt");
			this.outputDir = new Path("OUTPUT/youtube");
		} else {
			this.numReducers = Integer.parseInt(args[0]);
			this.inputPath = new Path(args[1]);
			this.outputDir = new Path(args[2]);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Finder(args), args);
		System.exit(res);
	}

}
