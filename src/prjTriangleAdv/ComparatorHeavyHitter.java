package prjTriangleAdv;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** Compare just the first element of the Pair */
public class ComparatorHeavyHitter extends WritableComparator {

	public ComparatorHeavyHitter() {
		super(BucketItem.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		if (a instanceof BucketItem && b instanceof BucketItem) {
			BucketItem la = (BucketItem) a;
			BucketItem lb = (BucketItem) b;
			if((la.getFromDegree()).equals(lb.getFromDegree())){
				return (la.getFrom()).compareTo(lb.getFrom());
			} else{
				return (la.getFromDegree()).compareTo(lb.getFromDegree());
			}
		}
		return super.compare(a, b);
	}

}