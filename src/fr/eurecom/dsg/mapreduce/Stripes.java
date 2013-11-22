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

import fr.eurecom.dsg.mapreduce.Pair.PairMapper;
import fr.eurecom.dsg.mapreduce.Pair.PairReducer;


public class Stripes extends Configured implements Tool {

  private int numReducers;
  private Path inputPath;
  private Path outputDir;

  @Override
  public int run(String[] args) throws Exception {
    
	  Configuration conf = getConf();
		Job job = new Job(conf, "PairNewAPI");

		job.setJarByClass(Stripes.class);

		job.setMapperClass(StripesMapper.class);
		job.setReducerClass(StripesReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringToIntMapWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringToIntMapWritable.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		
		FileInputFormat.addInputPath(job, this.inputPath);
		FileOutputFormat.setOutputPath(job, this.outputDir);		
		job.setNumReduceTasks(this.numReducers);
		

		job.waitForCompletion(true);
		

  return job.waitForCompletion(true) ? 0 : 1;
  }

  public Stripes (String[] args) {
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
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Stripes(args), args);
    System.exit(res);
  }
}

class StripesMapper
extends Mapper<LongWritable, Text,Text,StringToIntMapWritable> { 

  @Override
  public void map(LongWritable key, Text value, Context context)
  throws java.io.IOException, InterruptedException {
	  String line = value.toString();
		line = line.replaceAll("[^a-zA-Z0-9_]+", " ");
		line = line.replaceAll("^\\s+", "");
		String[] tokens = line.split("\\s+");
		StringToIntMapWritable h = new StringToIntMapWritable();
		for (int i = 0; i < tokens.length-1; i++) {
			h.clear();
			for (int j = Math.max(0, i - 1); j < Math.min(tokens.length,i + 2); j++) {
				if (i == j)
					continue;
				h.increment(tokens[j]);
			}
			context.write(new Text(tokens[i]), h);
		}
  }
}

class StripesReducer
extends Reducer<Text,StringToIntMapWritable,Text,StringToIntMapWritable> {
  @Override
  public void reduce(Text key, Iterable<StringToIntMapWritable> values,Context context) throws IOException, InterruptedException {

	  StringToIntMapWritable hf = new StringToIntMapWritable();
		for (StringToIntMapWritable value : values) {
			hf.sum(value);
		}
		context.write(key, hf);
  }
}