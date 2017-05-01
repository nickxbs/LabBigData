package prjTriangleNumericUndir;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
	private LongWritable from = new LongWritable();
	private LongWritable to = new LongWritable();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		line = line.replaceAll("^\\s+", "");
		String[] sp = line.split("\\s+");// splits on TAB
		if(!line.startsWith("#")) {
			Long lp0 = Long.parseLong(sp[0]);
			Long lp1 = Long.parseLong(sp[1]);
			if (lp0 != lp1) {
				if (lp0 < lp1) {
					from.set(lp0);
					to.set(lp1);
				} else {
					from.set(lp1);
					to.set(lp0);
				}
				context.write(from, to);// "1" link_to
			}
		}
	}

}
