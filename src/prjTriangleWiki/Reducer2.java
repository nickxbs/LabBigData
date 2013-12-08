package prjTriangleWiki;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<TextTriplet, Text, Text, Text> {
	private Text outText = new Text();

	@Override
	protected void reduce(TextTriplet key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		boolean ok = false;

		for (Text val : values) {
			String k = key.getTer().toString();
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
		if (first.compareTo(second) < 0 && first.compareTo(ter) <0 && second.compareTo(ter) <0) {
			outText.set(first + " " + second + " " + ter);
			context.write(outText, new Text());
			return;
		}
	}
}