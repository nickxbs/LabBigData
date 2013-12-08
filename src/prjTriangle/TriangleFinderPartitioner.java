package prjTriangle;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TriangleFinderPartitioner extends Partitioner<LongWritableTriplet, LongWritable> {
	@Override
	public int getPartition(LongWritableTriplet key, LongWritable value,	int numPartitions) {
		// TODO: implement getPartition such that pairs with the same first element
		//       will go to the same reducer. You can use toUnsigned as utility.
		return toUnsigned((key.getFirst().toString()+"|"+key.getSecond().toString()).hashCode())%numPartitions;
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