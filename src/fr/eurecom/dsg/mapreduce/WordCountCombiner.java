package fr.eurecom.dsg.mapreduce;

import java.io.IOException;

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

public class WordCountCombiner extends Configured implements Tool {

	static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split("\\s+");
			for(String word : words)
				context.write(new Text(word), new IntWritable(1));
		}

	}

	static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values)
				sum += value.get();
			context.write(key,new IntWritable(sum));
		}
	}

	  private int numReducers;
	  private Path inputPath;
	  private Path outputDir;
	  
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		
		Job job = new Job(conf,"Word Count with Combiner");
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(WCMapper.class);		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		/*
		 * Combiner
		 *  
		 * */
		
		job.setCombinerClass(WCReducer.class);
		
		FileInputFormat.addInputPath(job, this.inputPath);
		FileOutputFormat.setOutputPath(job, this.outputDir);		
		job.setNumReduceTasks(this.numReducers);
		
		job.setJarByClass(WordCountCombiner.class);

		job.waitForCompletion(true);
		
		return 0;
	}
	 public WordCountCombiner (String[] args) {
		    if (args.length != 3) {
		    	 this.numReducers = 2;
		    	    this.inputPath = new Path("/home/student/INPUT/text/quote.txt");
		    	    this.outputDir = new Path("/home/student/OUTPUT/wordcount/");
		    }
		    else{
		    this.numReducers = Integer.parseInt(args[0]);
		    this.inputPath = new Path(args[1]);
		    this.outputDir = new Path(args[2]);
		    }
		  }
	
	public static void main(String args[]) throws Exception {
		ToolRunner.run(new Configuration(), new WordCountCombiner(args), args);
	}
}
