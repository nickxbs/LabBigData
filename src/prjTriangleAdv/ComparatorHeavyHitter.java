package prjTriangleAdv;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Compare just the first element of the Pair
 */
public class ComparatorHeavyHitter extends WritableComparator {

    public ComparatorHeavyHitter() {
        super(BucketItem.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof BucketItemDegree && b instanceof BucketItemDegree) {
            BucketItemDegree la = (BucketItemDegree) a;
            BucketItemDegree lb = (BucketItemDegree) b;
            if((la.getFromDegree()).equals(lb.getFromDegree())){
                if((la.getFrom()).equals(lb.getFrom())){
                    return (la.getTypeRel()).compareTo(lb.getTypeRel());
                }
                else {
                    return new Integer(la.getFrom()).compareTo(lb.getFrom());
                }
            } else{
                return new Integer(la.getFromDegree()).compareTo(lb.getFromDegree());
            }
        }
        return super.compare(a, b);
    }

}