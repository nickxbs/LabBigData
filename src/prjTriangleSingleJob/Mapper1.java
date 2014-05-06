package prjTriangleSingleJob;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends
		Mapper<LongWritable, Text, LongLongLongLong, LongWritable> {
	private LongWritable to = new LongWritable();
	private int buckets;

	
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		this.buckets = conf.getInt("b",3);
		
		String line = value.toString();
		line = line.replaceAll("^\\s+", "");
		String[] sp = line.split("\\s+");// splits on TAB
		Long lp0 = Long.parseLong(sp[0]);
		Long lp1 = Long.parseLong(sp[1]);
		if (lp0 != lp1) {
			if (lp0 < lp1) {
				SetContext(context, lp0, lp1);
			} else {
				SetContext(context, lp1, lp0);
			}
		}
	}

	private void SetContext(Context context, Long lp0, Long lp1)
			throws IOException, InterruptedException {
		to.set(lp1);
		for (long j = 0; j < buckets; j++) {
			context.write(new LongLongLongLong("A",lp0 % buckets, lp1 % buckets, j, lp0), to);// "1"
			context.write(new LongLongLongLong("B",lp0 % buckets, j, lp1 % buckets, lp0), to);// "1"
			context.write(new LongLongLongLong("C",j, lp0 % buckets, lp1 % buckets, lp0), to);// "1"
																				

		}
	}

}