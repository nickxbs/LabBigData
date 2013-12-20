package prjTriangleUndir;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<TextPair, Text, Text, Text> {

	private List<String> partialJoin = new LinkedList<String>();
	private Text outText = new Text();

	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		partialJoin.clear();
		Text k = key.getFirst();
		String val = "";
		String direction = "";
		for (Text valText : values) {
			val = valText.toString();
			direction = key.getSecond().toString();
			if (direction.equals("0"))// link from
			{
				if (!partialJoin.contains(val))
					partialJoin.add(val);
			} else // link to
			{
				WriteContext(k, context, val);
			}
		}

	}

	private void WriteContext(Text key, Context context, String value)
			throws IOException, InterruptedException {
		for (String val : partialJoin) {
			if ((val.compareTo(key.toString()) < 0 && key.toString().compareTo(value) < 0)) {
				outText.set(val + " " + value);
				context.write(outText, key);
			}
		}
	}
}