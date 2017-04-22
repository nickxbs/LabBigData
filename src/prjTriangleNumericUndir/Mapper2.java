package prjTriangleNumericUndir;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<LongWritable, Text, LongLongBit, LongWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		line = line.replaceAll("^\\s+", "");
		String[] sp = line.split("\\s+");
		Long lp0=Long.parseLong(sp[0]);
		Long lp1=Long.parseLong(sp[1]);
		
		if (sp.length > 2) {
			context.write(new LongLongBit(lp0, lp1, true),new LongWritable(Long.parseLong(sp[2])));
		} else {
			if (lp0<lp1)
				context.write(new LongLongBit(lp0, lp1, false), new LongWritable(Long.parseLong("0"))); // li
			else
				context.write(new LongLongBit(lp1, lp0, false), new LongWritable(Long.parseLong("0"))); // li
		}
	}
}
