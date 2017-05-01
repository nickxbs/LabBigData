package prjTriangleNumericUndir;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partitioner1 extends Partitioner<LongWritable, LongWritable> {
	@Override
	public int getPartition(LongWritable from, LongWritable to,	int numPartitions) {
		return toUnsigned((from.toString()).hashCode())%numPartitions;
	}

	/**
	 * toUnsigned(10) = 10
	 * toUnsigned(-1) = 2147483647
	 * 
	 * @param val Value to convert
	 * @return the unsigned number with the same bits of val 
	 * */
	public static int toUnsigned(int val) {
		return val & Integer.MAX_VALUE;
	}

}