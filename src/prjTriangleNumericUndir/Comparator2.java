package prjTriangleNumericUndir;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** Compare just the first element of the Pair */
public class Comparator2 extends WritableComparator {

	public Comparator2() {
		super(LongLongBit.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		if (a instanceof LongLongBit && b instanceof LongLongBit) {
			LongLongBit la = (LongLongBit) a;
			LongLongBit lb = (LongLongBit) b;

			if (la.getFirst().get()<lb.getFirst().get())
				return -1;
			if (la.getFirst().get()>lb.getFirst().get())
				return 1;
			if (la.getSecond().get()<lb.getSecond().get())
				return -1;
			if (la.getSecond().get()>lb.getSecond().get())
				return 1;
			if(!la.getTer().get())
				return -1;
			if(la.getTer().get()==la.getTer().get())
				return 0;
			if(!la.getTer().get())
				return -1;
			if(la.getTer().get())
				return 1;
		}
		return super.compare(a, b);
	}

}