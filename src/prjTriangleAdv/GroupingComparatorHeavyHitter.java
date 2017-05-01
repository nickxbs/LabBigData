package prjTriangleAdv;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparatorHeavyHitter extends WritableComparator {

    public GroupingComparatorHeavyHitter() {
        super(BucketItem.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof BucketItem && b instanceof BucketItem) {
            BucketItem la = (BucketItem) a;
            BucketItem lb = (BucketItem) b;

            if (la.getBucketIndex().equals(lb.getBucketIndex())){
                return 0;
            } else{
                return (la.getBucketIndex().compareTo(lb.getBucketIndex()));
            }
        }
        return super.compare(a, b);
    }

}