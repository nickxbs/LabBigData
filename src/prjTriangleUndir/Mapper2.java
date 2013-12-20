package prjTriangleUndir;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<LongWritable, Text, TextTriplet, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		line = line.replaceAll("^\\s+", "");
		String[] sp = line.split("\\s+");

		if (sp.length > 2) {
			{
				if (sp[0].compareTo(sp[1]) != 0) {
					if (sp[0].compareTo(sp[1]) < 0)
						context.write(new TextTriplet(sp[0], sp[1], "1"),new Text(sp[2]));
					else
						context.write(new TextTriplet(sp[0], sp[1], "1"),new Text(sp[2]));
				}
			}
		} else {
			if (sp[0].compareTo(sp[1]) < 0)
				context.write(new TextTriplet(sp[0], sp[1], "0"), new Text("0")); // li
																					// inverto
																					// per
																					// match
																					// key
			else
				context.write(new TextTriplet(sp[1], sp[0], "0"), new Text("0")); // li
																					// inverto
																					// per
																					// match
																					// key

		}
	}
}
