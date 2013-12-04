package fr.eurecom.dsg.mapreduce.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReduceSideJoinReducer extends
		Reducer<TextPair, IntWritable, Text, IntWritable> {
	protected List<Integer> follower = new ArrayList<Integer>();

	@Override
	protected void reduce(TextPair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		// List<Integer> follower = new ArrayList<Integer>();
		if (key.getSecond().toString().equals("1") && !follower.isEmpty()) {

			for (IntWritable value : values) {
				for (Integer f : follower)
					context.write(new Text(f.toString()),
							new IntWritable(value.get()));
			}
		} else {
			follower.clear();
			for (IntWritable value : values) {
				follower.add(value.get());
			}
		}
	}
}