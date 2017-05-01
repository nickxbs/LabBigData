package prjTriangleAdv;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;

public class ReducerOthers extends
		Reducer<BucketItem, IntWritable, Text, Text> {
	private static final Log _log = LogFactory.getLog(ReducerOthers.class);

	private HashSet<Integer> nodes = new HashSet<Integer>();
	private HashSet<Pair<Integer, Integer>> edges = new HashSet<Pair<Integer, Integer>>();
	private Hashtable<Integer,ArrayList<Integer>> adjacent= new Hashtable<Integer, ArrayList<Integer>>();

	@Override
	protected void reduce(BucketItem bucketItem, Iterable<IntWritable> values,
						  Context context) throws IOException, InterruptedException {

		Iterator<IntWritable> it = values.iterator();

		//WriteDebug("\t NEW",context);
		while(it.hasNext()) {

			int from =bucketItem.getFrom().get();
			IntWritable toW=it.next();
			int to= toW.get();
			//WriteDebug(new Integer(from).toString()+"\t"+new Integer(to).toString()+"\t"+new Integer(bucketItem.getFromDegree().get()).toString() ,context);
			//Populate edge
			Pair myedge=new Pair(from,to);
			if(!edges.contains(myedge)){
				edges.add(myedge);
			}
			if(!nodes.contains(from)){
				nodes.add(from);
				ArrayList<Integer> toList=new ArrayList<Integer>();
				toList.add(to);
				adjacent.put(from,toList);
			} else{
				ArrayList<Integer> fromAdjecent=adjacent.get(from);
				if(!fromAdjecent.contains(to)){
					fromAdjecent.add(to);
				}
			}
		}
	}

	private void WriteContext(Integer a, Integer b, Integer c, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(a.toString()+ "\t"+b.toString()+ "\t"+c.toString()+ "\t"), new Text());
	}
	private void WriteDebug(String a, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(a.toString()), new Text());
	}
}
