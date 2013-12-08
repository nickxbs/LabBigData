package prjTriangle;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TriangleFinderReducer2 extends
		Reducer<LongWritableTriplet, LongWritable, Text, Text> {
	private Text outText = new Text();
	
	@Override
	protected void reduce(LongWritableTriplet key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
		boolean ok=false;

		for (LongWritable val : values) {
			if(val.get()==0){
				ok=true;
			}
			else{
				if(ok)
					WriteContext(key.getFirst().get(),key.getSecond().get(),val.get(),context);
				else
					return;
			}				
		}
	}

	private void WriteContext(Long first,Long second,Long val ,Context context) throws IOException, InterruptedException {
		if(val<first)
		{
			outText.set(val+" "+first+" "+second);
			context.write(outText, new Text());
			return;
		}
		else if(val>second)
		{
			outText.set(first+" "+second+" "+val);
			context.write(outText, new Text());
			return;

		}
		else if(!val.equals(first) && !val.equals(second))
		{
			outText.set(first+" "+val+" "+second);
			context.write(outText, new Text());
			return;

		}
	}	
}