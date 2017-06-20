package prjTriangleAdv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperOthers2 extends
		Mapper<LongWritable, Text, KeyClosure, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if(!line.contains("#")){
			line = line.replaceAll("^\\s+", "");
			String[] sp = line.split("\\s+");// splits on TAB
			int bucketIndex = Integer.parseInt(sp[0]);
			int b = Integer.parseInt(sp[1]);
			int c = Integer.parseInt(sp[2]);
			int a = Integer.parseInt(sp[3]);

			context.write(new KeyClosure(bucketIndex,b,c,a), new IntWritable(a));

		}
	}

}
