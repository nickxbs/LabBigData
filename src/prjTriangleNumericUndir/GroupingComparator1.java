package prjTriangleNumericUndir;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator1 extends WritableComparator {

	public GroupingComparator1() {
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
		return 0;
		}

		return super.compare(a, b);
	}

}