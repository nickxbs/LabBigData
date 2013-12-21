package prjTriangleNumericUndir;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** Compare just the first element of the Pair */
public class Comparator1 extends WritableComparator {

	public Comparator1() {
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
			// da qui i primi 2 sono =
			if(la.getSecond().get()==lb.getSecond().get())
				return 0;
			if(la.getSecond().get())
					return 1;
			if(!la.getSecond().get())
				return -1;
		
		}
		return super.compare(a, b);
	}

}