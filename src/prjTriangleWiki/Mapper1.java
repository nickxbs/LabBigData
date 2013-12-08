package prjTriangleWiki;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class Mapper1 extends Mapper<LongWritable, Text, TextPair, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		line = line.replaceAll("^\\s+", "");
		String[] sp = line.split("\\s+");// splits on TAB

		context.write(new TextPair(sp[1], "0"), new Text(sp[0]));// "0" link_from
		context.write(new TextPair(sp[0], "1"), new Text(sp[1]));// "1" link_to
	}
}
