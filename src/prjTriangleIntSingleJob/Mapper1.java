package prjTriangleIntSingleJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper1 extends
		Mapper<LongWritable, Text, BucketItem, IntWritable> {
	private IntWritable toWritable = new IntWritable();
	private int buckets;

	
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		this.buckets = conf.getInt("b",2);
		
		String line = value.toString();
		if(!line.contains("#")){
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
	}

	private void SetContext(Context context, int from, int to)
			throws IOException, InterruptedException {
		toWritable.set(to);
		for (int j = 0; j < buckets; j++) {
				int aIndex= (int) (((Math.pow(buckets,2))* (from % buckets))+(buckets*(to % buckets))+j);
				context.write(new BucketItem("A", aIndex, from), toWritable);
				int bIndex = (int) (((Math.pow(buckets, 2)) * (from % buckets)) + (buckets * (j)) + (to % buckets));
				context.write(new BucketItem("B", bIndex, from), toWritable);
				int cIndex= (int) (((Math.pow(buckets,2))* (j			 ))+(buckets*(from % buckets))+(to % buckets));
				context.write(new BucketItem("C", cIndex, from), toWritable);
		}
	}

}