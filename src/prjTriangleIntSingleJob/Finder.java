package prjTriangleIntSingleJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.javatuples.Pair;
import org.javatuples.Triplet;

//hadoop jar TriangleWiki.jar prjTriangleWiki.Finder 1 INPUT/ttter/twitter-big-sample.txt OUTPUT/twitterBig
//hadoop jar Triangle.jar prjTriangle.TriangleFinder 1 INPUT/twitter/twitter-small.txt OUTPUT/twitter

public class Finder extends Configured implements Tool {

	private Path outputDir;
	private Path inputPath;
	private int b;

	public int run(String[] args) throws Exception {

		Job job = new Job(super.getConf());
		job.setJarByClass(Finder.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(Mapper1.class);
		job.setMapOutputKeyClass(BucketItem.class);
		job.setMapOutputValueClass(IntWritable.class);
		Path in = this.inputPath;
		FileInputFormat.addInputPath(job, in);
		
		job.setSortComparatorClass(Comparator1.class);
		job.setGroupingComparatorClass(GroupingComparator1.class);
		//job.setPartitionerClass(Partitioner1.class);

		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);


		job.setNumReduceTasks(this.b*this.b*this.b);
		job.setOutputFormatClass(TextOutputFormat.class);


		// SequenceFileInputFormat.addInputPath(job2, in2);
		FileSystem dfs = FileSystem.get(getConf());
		if (dfs.exists(this.outputDir))
			dfs.delete(this.outputDir, true);
		FileOutputFormat.setOutputPath(job, this.outputDir);

		job.waitForCompletion(true);

		return 1;
	}

	public Finder() {
			this.b=1;
			this.inputPath = new Path("INPUT/twitter/twitter-small.txt");
			this.outputDir = new Path("OUTPUT/twitter-small");

//		this.b=10;
//		this.inputPath = new Path("INPUT/twitter/twitter-big-sample.txt");
//		this.outputDir = new Path("OUTPUT/twitterbig");

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Finder(), args);
		System.exit(res);
	}

}
