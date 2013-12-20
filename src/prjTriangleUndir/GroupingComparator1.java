package prjTriangleUndir;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator1 extends WritableComparator {

	public GroupingComparator1() {
		super(TextPair.class, true);
	}


	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		if (a instanceof TextPair && b instanceof TextPair) {
			TextPair la = (TextPair) a;
			TextPair lb = (TextPair) b;

			if (!la.getFirst().equals(lb.getFirst()))
				return (la.getFirst().compareTo(lb.getFirst()));
		return 0;
		}

		return super.compare(a, b);
	}

}