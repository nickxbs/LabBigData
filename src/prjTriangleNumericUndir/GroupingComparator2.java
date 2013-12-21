package prjTriangleNumericUndir;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator2 extends WritableComparator {

	public GroupingComparator2() {
		super(LongLongBit.class, true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		if (a instanceof LongLongBit && b instanceof LongLongBit) {
			LongLongBit la = (LongLongBit) a;
			LongLongBit lb = (LongLongBit) b;

			if (!la.getFirst().equals(lb.getFirst()))
				return (la.getFirst().compareTo(lb.getFirst()));
			else {
				if (!la.getSecond().equals(lb.getSecond()))
					return (la.getSecond().compareTo(lb.getSecond()));
				else {
					return 0;
				}

			}
		}

		return super.compare(a, b);
	}
}