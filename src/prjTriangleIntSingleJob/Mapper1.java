package prjTriangleIntSingleJob;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends
		Mapper<LongWritable, Text, BucketItem, LongWritable> {
	private LongWritable to = new LongWritable();
	private int buckets;

	
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		this.buckets = conf.getInt("b",2);
		
		String line = value.toString();
		line = line.replaceAll("^\\s+", "");
		String[] sp = line.split("\\s+");// splits on TAB
		int lp0 = Integer.parseInt(sp[0]);
		int lp1 = Integer.parseInt(sp[1]);
		if (lp0 != lp1) {
			if (lp0 < lp1) {
				SetContext(context, lp0, lp1);
			} else {
				SetContext(context, lp1, lp0);
			}
		}
	}

	private void SetContext(Context context, int lp0, int lp1)
			throws IOException, InterruptedException {
		to.set(lp1);
		for (int j = 0; j < buckets; j++) {
			context.write(new BucketItem("A",lp0 % buckets, lp1 % buckets, j, lp0), to);// "1"
			context.write(new BucketItem("B",lp0 % buckets, j, lp1 % buckets, lp0), to);// "1"
			context.write(new BucketItem("C",j, lp0 % buckets, lp1 % buckets, lp0), to);// "1"
																				

		}
	}

}
