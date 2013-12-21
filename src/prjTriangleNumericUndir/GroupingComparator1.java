package prjTriangleNumericUndir;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator1 extends WritableComparator {

	public GroupingComparator1() {
		super(LongBit.class, true);
	}


	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		if (a instanceof LongBit && b instanceof LongBit) {
			LongBit la = (LongBit) a;
			LongBit lb = (LongBit) b;

			if (la.getFirst().get()<lb.getFirst().get())
				return -1;
			if (la.getFirst().get()>lb.getFirst().get())
				return 1;
		return 0;
		}

		return super.compare(a, b);
	}

}