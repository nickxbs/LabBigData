package prjTriangleNumericUndir;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partitioner1 extends Partitioner<LongBit, LongWritable> {
	@Override
	public int getPartition(LongBit key, LongWritable value,	int numPartitions) {
		return toUnsigned((key.getFirst().toString()).hashCode())%numPartitions;
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