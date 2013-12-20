package prjTriangleUndir;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<TextTriplet, Text, Text, Text> {
	private Text outText = new Text();
	private List<String> vals = new LinkedList<String>();

	@Override
	protected void reduce(TextTriplet key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		boolean ok = false;
		vals.clear();
		String k = key.getTer().toString();
		for (Text val : values) {
			k = key.getTer().toString();//forse da togliere
			if (k.equals("0")) {
				ok = true;
			} else {
				if (ok)
					WriteContext(key.getFirst().toString(), val.toString(), key.getSecond().toString(), context);
				else
					return;
			}
		}
	}

	private void WriteContext(String first, String second, String ter,
			Context context) throws IOException, InterruptedException {
		// per evitare duplicazioni
		if (((first.compareTo(second) < 0 && second.compareTo(ter) <0) || (first.compareTo(second) > 0  && second.compareTo(ter) >0)) && !vals.contains(second)) {
			outText.set(first + " " + second + " " + ter);
			context.write(outText, new Text());
			vals.add(second);
			return;
		}
	}
}