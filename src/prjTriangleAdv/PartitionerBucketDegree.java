package prjTriangleAdv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerBucketDegree extends Partitioner<BucketItemDegree, Writable> {


	public int getPartition(BucketItemDegree bucketItem, Writable intWritable, int numPartitions) {

		return bucketItem.getBucketIndex()%numPartitions;
	}
}