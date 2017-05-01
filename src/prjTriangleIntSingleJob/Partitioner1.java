package prjTriangleIntSingleJob;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partitioner1 extends Partitioner<BucketItem, IntWritable> {


	public int getPartition(BucketItem bucketItem, IntWritable intWritable, int numPartitions) {

		return bucketItem.getBucketIndex().get()%numPartitions;
	}
}