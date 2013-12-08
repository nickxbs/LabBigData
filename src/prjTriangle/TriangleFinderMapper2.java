package prjTriangle;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TriangleFinderMapper2 extends
		Mapper<LongWritable, Text,  LongWritableTriplet,LongWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		line = line.replaceAll("^\\s+", "");
		String[] sp = line.split("\\s+");

		Long min= Math.min(Long.parseLong(sp[0]), Long.parseLong(sp[1]));
		Long max= Math.max(Long.parseLong(sp[0]), Long.parseLong(sp[1]));

		if(sp.length>2){
			Long val=Long.parseLong(sp[2]);
			{
				context.write(new LongWritableTriplet(min,max,new Long(1)),new LongWritable(val));return;
			}			
		}
		else
		{
			context.write(new LongWritableTriplet(min, max,new Long(0)),new LongWritable(0));			
		}
	}
}
