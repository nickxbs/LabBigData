package prjTriangleAdv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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

//hadoop jar TriangleWiki.jar prjTriangleWiki.Finder 1 INPUT/ttter/twitter-big-sample.txt OUTPUT/twitterBig
//hadoop jar Triangle.jar prjTriangle.TriangleFinder 1 INPUT/twitter/twitter-small.txt OUTPUT/twitter

public class Finder extends Configured implements Tool {

	private Path outputDir;
	private Path inputPath;
	private Path partialDir;
	private int b;



	public int run(String[] args) throws Exception {
		FileSystem dfs = FileSystem.get(getConf());
		Path inSource = this.inputPath;
		Path outPartial= new Path(this.partialDir.toString());

		Path outDegree = new Path(outPartial.toString() + "/degree");
		Path tmpoutDegree = new Path(this.partialDir.toString() + "_partial-degree");

		Path outCountPath = new Path(outPartial.toString() + "/countedges");
		Path tmpoutCountPath = new Path(this.partialDir.toString() + "_partial-countnodes");

		Path inPartial= new Path(this.partialDir.toString() + "_in-partial");
		Path outHH= new Path(this.outputDir.toString() + "_out-hh");
		Path outOther= new Path(this.outputDir.toString() + "_out-ot");
/*
		Job jobCountNodes = Job.getInstance(super.getConf(), "jobCountEdges");
		jobCountNodes.setJarByClass(Finder.class);

		jobCountNodes.setInputFormatClass(TextInputFormat.class);

		jobCountNodes.setMapperClass(MapperCount.class);
		jobCountNodes.setMapOutputKeyClass(IntWritable.class);
		jobCountNodes.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(jobCountNodes, inSource);

		jobCountNodes.setReducerClass(ReducerCount.class);
		jobCountNodes.setOutputKeyClass(Text.class);
		jobCountNodes.setOutputValueClass(Text.class);

		jobCountNodes.setNumReduceTasks(this.b);
		jobCountNodes.setOutputFormatClass(TextOutputFormat.class);



		if (dfs.exists(tmpoutCountPath))
			dfs.delete(tmpoutCountPath, true);
		FileOutputFormat.setOutputPath(jobCountNodes, tmpoutCountPath);
		jobCountNodes.waitForCompletion(true);
		if (dfs.exists(outCountPath))
			dfs.delete(outCountPath, true);
		FileUtil.copyMerge(dfs,tmpoutCountPath,dfs,outCountPath, false,getConf(),"");

		Job jobDegree = Job.getInstance(super.getConf(), "RunDegree");
		jobDegree.setJarByClass(Finder.class);

		jobDegree.setInputFormatClass(TextInputFormat.class);

		jobDegree.setMapperClass(MapperDegree.class);
		jobDegree.setMapOutputKeyClass(IntWritable.class);
		jobDegree.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(jobDegree, inSource);
		
		jobDegree.setGroupingComparatorClass(GroupingComparatorDegree.class);
		jobDegree.setPartitionerClass(PartitionerDegree.class);

		jobDegree.setReducerClass(ReducerDegree.class);
		jobDegree.setOutputKeyClass(Text.class);
		jobDegree.setOutputValueClass(Text.class);

		jobDegree.setNumReduceTasks(this.b);
		jobDegree.setOutputFormatClass(TextOutputFormat.class);

		if (dfs.exists(tmpoutDegree))
			dfs.delete(tmpoutDegree, true);

		FileOutputFormat.setOutputPath(jobDegree, tmpoutDegree);
		jobDegree.waitForCompletion(true);
		if (dfs.exists(outDegree))
			dfs.delete(outDegree, true);
		FileUtil.copyMerge(dfs,tmpoutDegree,dfs,outDegree, false,getConf(),"");
*/


		Job jobHH = Job.getInstance(super.getConf(), "jobHH");
		jobHH.setJarByClass(Finder.class);

		jobHH.setInputFormatClass(TextInputFormat.class);

		jobHH.setMapperClass(MapperHeavyHitter.class);
		jobHH.setMapOutputKeyClass(BucketItem.class);
		jobHH.setMapOutputValueClass(BucketItem.class);

		//jobHH.setGroupingComparatorClass(GroupingComparatorHeavyHitter.class);
		jobHH.setPartitionerClass(PartitionerBucket.class);
		jobHH.setSortComparatorClass(ComparatorHeavyHitter.class);

		jobHH.setReducerClass(ReducerOthers.class);
		jobHH.setOutputKeyClass(Text.class);
		jobHH.setOutputValueClass(Text.class);

		jobHH.setNumReduceTasks(((int) Math.pow(this.b, 3)));
		jobHH.setOutputFormatClass(TextOutputFormat.class);

		if (dfs.exists(inPartial))
			dfs.delete(inPartial, true);
		FileUtil.copy(dfs,inSource,dfs,new Path(outPartial.toString() + "/source"),false,getConf());
		FileUtil.copyMerge(dfs,outPartial,dfs,inPartial, false,getConf(),"");


		FileInputFormat.addInputPath(jobHH, inPartial);



		if (dfs.exists(outHH))
			dfs.delete(outHH, true);
		FileOutputFormat.setOutputPath(jobHH, outHH);
		jobHH.waitForCompletion(true);



		Job jobOthers = Job.getInstance(super.getConf(), "jobOthers");
		jobOthers.setJarByClass(Finder.class);

		jobOthers.setInputFormatClass(TextInputFormat.class);

		jobOthers.setMapperClass(MapperOthers.class);
		jobOthers.setMapOutputKeyClass(BucketItem.class);
		jobOthers.setMapOutputValueClass(BucketItem.class);

		//jobOthers.setGroupingComparatorClass(GroupingComparatorHeavyHitter.class);
		jobOthers.setPartitionerClass(PartitionerBucket.class);
		jobOthers.setSortComparatorClass(ComparatorHeavyHitter.class);

		jobOthers.setReducerClass(ReducerOthers.class);
		jobOthers.setOutputKeyClass(Text.class);
		jobOthers.setOutputValueClass(Text.class);

		jobOthers.setNumReduceTasks(((int) Math.pow(this.b, 3)));
		jobOthers.setOutputFormatClass(TextOutputFormat.class);

		if (dfs.exists(inPartial))
			dfs.delete(inPartial, true);
		FileUtil.copy(dfs,inSource,dfs,new Path(outPartial.toString() + "/source"),false,getConf());
		FileUtil.copyMerge(dfs,outPartial,dfs,inPartial, false,getConf(),"");


		FileInputFormat.addInputPath(jobOthers, inPartial);

		if (dfs.exists(outOther))
			dfs.delete(outOther, true);
		FileOutputFormat.setOutputPath(jobOthers, outOther);
		jobOthers.waitForCompletion(true);

		return 1;
	}

	public Finder() {
			this.b=2;
			//this.inputPath = new Path("INPUT/twitter/twitter-verysmall.txt");
			//this.outputDir = new Path("OUTPUT/twitter-verysmall");
			//this.partialDir = new Path("PARTIAL/twitter-verysmall");

			//this.inputPath = new Path("INPUT/roads/roadNet-CA.txt");
			//this.outputDir = new Path("OUTPUT/roadNet-CA");

		this.inputPath = new Path("INPUT/youtube/youtube.txt");
		this.outputDir = new Path("OUTPUT/youtube");
		this.partialDir = new Path("PARTIAL/youtube");


//		this.b=10;
//		this.inputPath = new Path("INPUT/twitter/twitter-big-sample.txt");
//		this.outputDir = new Path("OUTPUT/twitterbig");

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Finder(), args);
		System.exit(res);
	}

}
