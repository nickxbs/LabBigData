package prjTriangleAdv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerBucket extends Partitioner<BucketItem, Writable> {


	public int getPartition(BucketItem bucketItem, Writable intWritable, int numPartitions) {

		return bucketItem.getBucketIndex().get()%numPartitions;
	}
}