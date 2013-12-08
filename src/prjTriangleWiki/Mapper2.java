package prjTriangleWiki;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends
		Mapper<LongWritable, Text,  TextTriplet,Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		line = line.replaceAll("^\\s+", "");
		String[] sp = line.split("\\s+");

		

		if(sp.length>2){
			{
				context.write(new TextTriplet(sp[0],sp[1],"1"),new Text(sp[2]));return;
			}			
		}
		else
		{
			context.write(new TextTriplet(sp[1],sp[0],"0"),new Text("0")); //li inverto per match key			
		}
	}
}
