package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

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



public class WordCountIMC extends Configured implements Tool {

	  private int numReducers;
	  private Path inputPath;
	  private Path outputDir;
	  
	static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private HashMap<String, Integer> partialResults;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			this.partialResults = new HashMap<String, Integer>();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] words = line.split("\\s+");
			for(String word : words) {				
				if (this.partialResults.containsKey(word))
					this.partialResults.put(word, this.partialResults.get(word)+1);
				else
					this.partialResults.put(word, 1);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
		
			for (Entry<String, Integer> entry : this.partialResults.entrySet())
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			this.partialResults.clear();
			
			super.cleanup(context);
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

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		
		Job job = new Job(conf,"Word Count in memory combiner");
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(WCMapper.class);		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);		

		FileInputFormat.addInputPath(job, this.inputPath);
		FileOutputFormat.setOutputPath(job, this.outputDir);
		
		job.setNumReduceTasks(this.numReducers);
		
		job.setJarByClass(WordCountIMC.class);

		job.waitForCompletion(true);
		
		return 0;
	}
	 public WordCountIMC (String[] args) {
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
		ToolRunner.run(new Configuration(), new WordCountIMC(args), args);
	}
}

