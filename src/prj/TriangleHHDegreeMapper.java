package prj;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TriangleHHDegreeMapper extends
		Mapper<LongWritable, Text, LongWritable, LongWritablePair> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// 9465097 12566713
		String line = value.toString();
		line = line.replaceAll("^\\s+", "");
		String[] sp = line.split("\\s+");// splits on TAB

		LongWritablePair tp = new LongWritablePair(Long.parseLong(sp[0]), Long.parseLong(sp[1]));
		context.write(new LongWritable (Math.min(Long.parseLong(sp[0]), Long.parseLong(sp[1]))), tp);

	}
}