package fr.eurecom.dsg.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PageRankReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String nodes = "";
		Text word = new Text();
		Double s = (double) 0;

		for (Text val : values) {// looks like NODES/VALUES 1 0 2:3:, we need to
									// use the first as a key
			String[] sp = val.toString().split(" ");// splits on space
			// look at first value
			if (sp[0].equalsIgnoreCase("NODES")) {
				nodes = null;
				nodes = val.toString().replaceAll("NODES ", "");
			} else if (sp[0].equalsIgnoreCase("VALUE")) {
				s = s + Double.parseDouble(sp[1]);
			}
		}
		word.set(s + " " + nodes);
		context.write(key, word);
		word.clear();
	}
}