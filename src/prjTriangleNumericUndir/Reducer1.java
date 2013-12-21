package prjTriangleNumericUndir;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<LongBit, LongWritable, LongLong, LongWritable> {

	private List<Long> partialJoin = new LinkedList<Long>();
	private LongLong outText = new LongLong();

	@Override
	protected void reduce(LongBit key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		partialJoin.clear();
		LongWritable k = key.getFirst();

		for (LongWritable valText : values) {
			Long val = valText.get();
			if (!key.getSecond().get())// link from
			{
				if (!partialJoin.contains(val))
					partialJoin.add(val);
			} else // link to
			{
				WriteContext(k, context, val);
			}
		}

	}

	private void WriteContext(LongWritable key, Context context, Long value)
			throws IOException, InterruptedException {
		for (Long val : partialJoin) {
			if (val < key.get() && key.get()<value) {
				outText.set(val, value);
				context.write(outText, key);
			}
		}
	}
}