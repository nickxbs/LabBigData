package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import fr.eurecom.dsg.mapreduce.WordCountCombiner.WCMapper;
import fr.eurecom.dsg.mapreduce.WordCountCombiner.WCReducer;


public class Pair extends Configured implements Tool {



  public static class PairMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
  
  
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	  String line = value.toString();
		line = line.replaceAll("[^a-zA-Z0-9_]+", " ");
		line = line.replaceAll("^\\s+", "");
		String[] words = line.split("\\s+");
		int i =0;
		for(String word : words)
		{
			if(i>0)
				context.write(new TextPair(words[i-1],word), new IntWritable(1));
			i++;
		}
	}	
}


  public static class PairReducer
    extends Reducer<TextPair, // TODO: change Object to input key type
    IntWritable, // TODO: change Object to input value type
    TextPair, // TODO: change Object to output key type
    IntWritable> { // TODO: change Object to output value type

	  @Override
		protected void reduce(TextPair key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values)
				sum += value.get();
			context.write(key,new IntWritable(sum));
		}
	  
  }

  private int numReducers;
  private Path inputPath;
  private Path outputDir;

  public Pair(String[] args) {
	    if (args.length != 3) {
	    	 this.numReducers = 2;
	    	    this.inputPath = new Path("/home/student/INPUT/text/quote.txt");
	    	    this.outputDir = new Path("/home/student/OUTPUT/wordcountPair/");
	    }
	    else{
	    this.numReducers = Integer.parseInt(args[0]);
	    this.inputPath = new Path(args[1]);
	    this.outputDir = new Path(args[2]);
	    }
  }
  

  @Override
  public int run(String[] args) throws Exception {


		Configuration conf = getConf();
		Job job = new Job(conf, "PairNewAPI");

		job.setJarByClass(Pair.class);

		job.setMapperClass(PairMapper.class);
		job.setReducerClass(PairReducer.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		
		FileInputFormat.addInputPath(job, this.inputPath);
		FileOutputFormat.setOutputPath(job, this.outputDir);		
		job.setNumReduceTasks(this.numReducers);
		

		job.waitForCompletion(true);
		

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Pair(args), args);
    System.exit(res);
  }
}
