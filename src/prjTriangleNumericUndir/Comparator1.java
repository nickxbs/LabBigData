package prjTriangleNumericUndir;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** Compare just the first element of the Pair */
public class Comparator1 extends WritableComparator {

	public Comparator1() {
		super(LongWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		if (a instanceof LongWritable && b instanceof LongWritable) {
			LongWritable la = (LongWritable) a;
			LongWritable lb = (LongWritable) b;

			if (la.get()<lb.get())
				return -1;
			if (la.get()>lb.get())
				return 1;
		}
		return super.compare(a, b);
	}

}