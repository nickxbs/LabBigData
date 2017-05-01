package prjTriangleAdv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparatorDegree extends WritableComparator {

    public GroupingComparatorDegree() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof IntWritable && b instanceof IntWritable ) {
            IntWritable  la = (IntWritable) a;
            IntWritable  lb = (IntWritable ) b;
            return (la.compareTo(lb));
        }
        return super.compare(a, b);
    }

}