package prjTriangle;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TriangleFinderMapper extends
		Mapper<LongWritable, Text, LongWritable, LongWritablePair> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		line = line.replaceAll("^\\s+", "");
		String[] sp = line.split("\\s+");

		Long min= Math.min(Long.parseLong(sp[0]), Long.parseLong(sp[1]));
		Long max= Math.max(Long.parseLong(sp[0]), Long.parseLong(sp[1]));

		context.write(new LongWritable(min), new  LongWritablePair(min, max));
		context.write(new LongWritable(max), new LongWritablePair(min, max));
	}
}
