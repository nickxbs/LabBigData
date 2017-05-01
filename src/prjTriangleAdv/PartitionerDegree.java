package prjTriangleAdv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerDegree extends Partitioner<IntWritable, Text> {


	public int getPartition(IntWritable node, Text text, int numPartitions) {

		return node.get()%numPartitions;
	}
}