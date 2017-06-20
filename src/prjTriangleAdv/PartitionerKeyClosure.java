package prjTriangleAdv;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerKeyClosure extends Partitioner<KeyClosure, Writable> {


	public int getPartition(KeyClosure bucketItem, Writable intWritable, int numPartitions) {

		return bucketItem.getBucketIndex()%numPartitions;
	}
}