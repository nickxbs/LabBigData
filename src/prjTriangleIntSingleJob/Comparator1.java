package prjTriangleIntSingleJob;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** Compare just the first element of the Pair */
public class Comparator1 extends WritableComparator {

	public Comparator1() {
		super(BucketItem.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		if (a instanceof BucketItem && b instanceof BucketItem) {
			BucketItem la = (BucketItem) a;
			BucketItem lb = (BucketItem) b;
			return la.compareTo(lb);
		}
		return super.compare(a, b);
	}

}