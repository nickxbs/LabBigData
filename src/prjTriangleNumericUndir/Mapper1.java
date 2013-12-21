package prjTriangleNumericUndir;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, LongBit, LongWritable> {
	private LongBit textP = new LongBit();
	private LongWritable text = new LongWritable();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		line = line.replaceAll("^\\s+", "");
		String[] sp = line.split("\\s+");// splits on TAB
		Long lp0=Long.parseLong(sp[0]);
		Long lp1=Long.parseLong(sp[1]);
		if (lp0!= lp1) {
			if (lp0 < lp1) {
				textP.set(lp1, false);
				text.set(lp0);
				context.write(textP, text);// "0" link_from

				textP.set(lp0, true);
				text.set(lp1);
				context.write(textP, text);// "1" link_to
			} else {
				textP.set(lp0, false);
				text.set(lp1);
				context.write(textP, text);// "0" link_from

				textP.set(lp1, true);
				text.set(lp0);
				context.write(textP, text);// "1" link_to
			}
		}
	}

}
