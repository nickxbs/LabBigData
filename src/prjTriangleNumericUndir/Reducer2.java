package prjTriangleNumericUndir;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<LongLongBit, LongWritable, Text, Text> {
	private Text outText = new Text();
	private List<Long> vals = new LinkedList<Long>();

	@Override
	protected void reduce(LongLongBit key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		boolean ok = false;
		vals.clear();
		Boolean k = key.getTer().get();
		for (LongWritable val : values) {
			k = key.getTer().get();//forse da togliere
			if (!k) {
				ok = true;
			} else {
				if (ok)
					WriteContext(key.getFirst().get(), val.get(), key.getSecond().get(), context);
				else
					return;
			}
		}
	}

	private void WriteContext(Long first, Long second, Long ter,
			Context context) throws IOException, InterruptedException {
		// per evitare duplicazioni
		if (((first<second && second<ter) || (first> second  && second>ter)) && !vals.contains(second)) {
			outText.set(first + " " + second + " " + ter);
			context.write(outText, new Text());
			vals.add(second);
			return;
		}
	}
}