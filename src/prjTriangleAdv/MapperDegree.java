package prjTriangleAdv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperDegree extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String line = value.toString();
		if(!line.contains("#")){
			line = line.replaceAll("^\\s+", "");
			String[] sp = line.split("\\s+");// splits on TAB
			int lp0 = Integer.parseInt(sp[0]);
			int lp1 = Integer.parseInt(sp[1]);
			context.write(new IntWritable(lp0), new Text());
			context.write(new IntWritable(lp1), new Text());

		}
	}

}
