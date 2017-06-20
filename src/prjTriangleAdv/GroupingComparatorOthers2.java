package prjTriangleAdv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparatorOthers2 extends WritableComparator {

    public GroupingComparatorOthers2() {
        super(KeyClosure.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof KeyClosure && b instanceof KeyClosure ) {
            KeyClosure  la = (KeyClosure) a;
            KeyClosure  lb = (KeyClosure) b;
            if (la.getBucketIndex().equals(lb.getBucketIndex()) && la.getB().equals(lb.getB()) && la.getC().equals(lb.getC())){
                return 0;
            } else{
                return (la.compareTo(lb));
            }
        }
        return super.compare(a, b);
    }

}