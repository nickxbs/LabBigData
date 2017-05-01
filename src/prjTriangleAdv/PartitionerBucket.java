package prjTriangleAdv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerBucket extends Partitioner<BucketItem, IntWritable> {


	public int getPartition(BucketItem bucketItem, IntWritable intWritable, int numPartitions) {

		return bucketItem.getBucketIndex().get()%numPartitions;
	}
}