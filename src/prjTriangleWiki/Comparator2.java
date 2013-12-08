package prjTriangleWiki;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** Compare just the first element of the Pair */
public class Comparator2 extends WritableComparator {

	public Comparator2() {
		super(TextTriplet.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		if (a instanceof TextTriplet && b instanceof TextTriplet) {
			TextTriplet la = (TextTriplet) a;
			TextTriplet lb = (TextTriplet) b;

			if (!la.getFirst().equals(lb.getFirst()))
				return (la.getFirst().compareTo(lb.getFirst()));
			else {
				if (!la.getSecond().equals(lb.getSecond()))
					return (la.getSecond().compareTo(lb.getSecond()));
				else {
					if (!la.getTer().equals(lb.getTer()))
						return (la.getTer().compareTo(lb.getTer()));
				}

			}
		}

		return super.compare(a, b);
	}

}