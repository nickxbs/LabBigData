package prjTriangleIntSingleJob;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator1 extends WritableComparator {

	public GroupingComparator1() {
		super(BucketItem.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		if (a instanceof BucketItem && b instanceof BucketItem) {
			BucketItem la = (BucketItem) a;
			BucketItem lb = (BucketItem) b;

			if (!la.getFirst().equals(lb.getFirst()))
				return (la.getFirst().compareTo(lb.getFirst()));
			else {
				if (!la.getSecond().equals(lb.getSecond()))
					return (la.getSecond().compareTo(lb.getSecond()));
				else {
					if (!la.getthird().equals(lb.getthird()))
						return (la.getthird().compareTo(lb.getthird()));
					else {
						if (!la.getfourth().equals(lb.getfourth()))
							return (la.getfourth().compareTo(lb.getfourth()));
					}
				}
			}
			return 0;
		}
		return super.compare(a, b);
	}

}