package prjTriangleUndir;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, TextPair, Text> {
	private TextPair textP = new TextPair();
	private Text text = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		line = line.replaceAll("^\\s+", "");
		String[] sp = line.split("\\s+");// splits on TAB
		if (sp[0].compareTo(sp[1]) != 0) {
			if (sp[0].compareTo(sp[1]) < 0) {
				textP.set(sp[1], "0");
				text.set(sp[0]);
				context.write(textP, text);// "0" link_from

				textP.set(sp[0], "1");
				text.set(sp[1]);
				context.write(textP, text);// "1" link_to
			} else {
				textP.set(sp[0], "0");
				text.set(sp[1]);
				context.write(textP, text);// "0" link_from

				textP.set(sp[1], "1");
				text.set(sp[0]);
				context.write(textP, text);// "1" link_to
			}
		}
	}

}
