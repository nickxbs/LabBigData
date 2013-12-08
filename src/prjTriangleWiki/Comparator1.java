package prjTriangleWiki;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** Compare just the first element of the Pair */
public class Comparator1 extends WritableComparator {

	public Comparator1() {
		super(TextPair.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		if (a instanceof TextPair && b instanceof TextPair) {
			TextPair la = (TextPair) a;
			TextPair lb = (TextPair) b;

			if (!la.getFirst().equals(lb.getFirst()))
				return (la.getFirst().compareTo(lb.getFirst()));
			else {
				if (!la.getSecond().equals(lb.getSecond()))
					return (la.getSecond().compareTo(lb.getSecond()));
			}
		}
		return super.compare(a, b);
	}

}