package prj;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class TriangleComputeDegreeReducer extends
Reducer<LongWritable,IntWritable, LongWritable, IntWritable> {
	public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int t = 0;

		for (IntWritable val : values) {
			t+=val.get();
		}
		context.write(key, new IntWritable(t));
	}
}