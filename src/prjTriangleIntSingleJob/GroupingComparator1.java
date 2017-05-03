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

            if (la.getBucketIndex().equals(lb.getBucketIndex())){
                if (la.getTypeRel().equals(lb.getTypeRel())){
                    return 0;
                } else{
                    return (la.getTypeRel().compareTo(lb.getTypeRel()));
                }
            } else{
                return (la.getBucketIndex().compareTo(lb.getBucketIndex()));
            }
        }
        return super.compare(a, b);
    }

}