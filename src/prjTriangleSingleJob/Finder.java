package prjTriangleSingleJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// sudo /home/cloudera/cloudera-manager --force --express

//hadoop jar TriangleWiki.jar prjTriangleWiki.Finder 1 INPUT/ttter/twitter-big-sample.txt OUTPUT/twitterBig
//hadoop jar Triangle.jar prjTriangle.TriangleFinder 1 INPUT/twitter/twitter-small.txt OUTPUT/twitter
//hadoop jar LabBigData.jar   prjTriangleSingleJob.Finder 1 INPUT/twitter-verysmall.txt OUTPUT/twitter
//HADOOP_HEAPSIZE=2000 hadoop jar LabBigData.jar   prjTriangleSingleJob.Finder 3 INPUT/twitter-big-sample.txt OUTPUT/twitter-big2

public class Finder extends Configured implements Tool {

	private Path outputDir;
	private Path inputPath;
	private int b;
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		conf.setInt("b", this.b);
		//conf.setBoolean("mapred.compress.map.output",true);
		//conf.setClass("mapred.map.output.compression.codec", GzipCodec.class, CompressionCodec.class);

		Job job = Job.getInstance(conf);
		job.setJarByClass(Finder.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(Mapper1.class);
		job.setMapOutputKeyClass(LongLongLongLong.class);
		job.setMapOutputValueClass(LongWritable.class);
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

	public Finder(String[] args) {
			this.b=Integer.parseInt(args[0]);
			this.inputPath = new Path(args[1]);
			this.outputDir = new Path(args[2]);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Finder(args), args);
		System.exit(res);
	}
}
 