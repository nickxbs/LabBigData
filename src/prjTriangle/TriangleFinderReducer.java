package prjTriangle;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TriangleFinderReducer extends
		Reducer<LongWritable, LongWritablePair, Text, LongWritable> {


	private List<Long> partialJoin = new LinkedList<Long>();
	private Text outText = new Text();
	
	@Override
	protected void reduce(LongWritable key, Iterable<LongWritablePair> values,Context context) throws IOException, InterruptedException {

		Long k=key.get();
		Long first;
		Long second;
		for (LongWritablePair pair : values) {
			first=pair.getFirst().get();
			if(!k.equals(first))
			{
				WriteContext(key, context, first);
			}
			else
			{
				second=pair.getSecond().get();
				if(!k.equals(second)){
				WriteContext(key, context, second);
			}}
		}
		partialJoin.clear();
	}

	private void WriteContext(LongWritable key, Context context,Long value) throws IOException, InterruptedException {
		if(!partialJoin.contains(value)){
			for (Long val : partialJoin) {
				outText.set(val+" "+value);
				context.write(outText, key);
			}
		partialJoin.add(value);
		}
	}
}