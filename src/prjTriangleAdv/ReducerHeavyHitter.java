package prjTriangleAdv;

import com.sun.tools.javac.util.IntHashTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.*;

public class ReducerHeavyHitter extends
		Reducer<BucketItem, IntWritable, Text, Text> {
	private static final Log _log = LogFactory.getLog(ReducerHeavyHitter.class);

	private HashSet<Integer> nodeList = new HashSet<Integer>();
	private HashSet<Pair<Integer, Integer>> edges = new HashSet<Pair<Integer, Integer>>();
	private int oldNode=-1;
	private int deg=0;
	private String odlType="";

	@Override
	protected void reduce(BucketItem bucketItem, Iterable<IntWritable> values,
						  Context context) throws IOException, InterruptedException {

		Iterator<IntWritable> it = values.iterator();

		//WriteDebug("\t NEW",context);
		while(it.hasNext()) {

			int from =bucketItem.getFrom();
			IntWritable toW=it.next();
			int to= toW.get();
			//WriteDebug(new Integer(from).toString()+"\t"+new Integer(to).toString()+"\t"+new Integer(bucketItem.getFromDegree().get()).toString() ,context);
			//Populate edge
			Pair myedge=new Pair(from,to);
			if(!edges.contains(myedge)){
				edges.add(myedge);
			}
			if(!nodeList.contains(from)){
				nodeList.add(from);
			}

/*
			//Populate adjacent
			List<Integer> fromAdjecent=adjacent.get(from);
			if(fromAdjecent!=null){
				fromAdjecent.add(to);
			} else{
				List<Integer> myFromAdjacent= new ArrayList<Integer>();
				myFromAdjacent.add(from);
				adjacent.add(from,myFromAdjacent);
			}

			List<Integer> toAdjecent=adjacent.get(to);
			if(toAdjecent!=null){
				toAdjecent.add(from);
			} else{
				List<Integer> myToAdjacent= new ArrayList<Integer>();
				myToAdjacent.add(from);
				adjacent.add(from,myToAdjacent);
			}
			*/

		}
		for (Integer nodeA:nodeList) {
			for (Integer nodeB:nodeList) {
				for (Integer nodeC:nodeList) {
					if(nodeA>nodeB && nodeB>nodeC){
						Pair<Integer,Integer> p1= new Pair<Integer, Integer>(nodeA,nodeB);
						Pair<Integer,Integer> p2= new Pair<Integer, Integer>(nodeA,nodeC);
						Pair<Integer,Integer> p3= new Pair<Integer, Integer>(nodeB,nodeC);
						if(edges.contains(p1) && edges.contains(p2) && edges.contains(p3)){
							WriteContext(nodeA,nodeB,nodeC,context);
						}
					}
				}

			}
			
		}
		
	}

	private void WriteContext(Integer a, Integer b, Integer c, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(a.toString()+ "\t"+b.toString()+ "\t"+c.toString()+ "\t"), new Text());
	}
/*
	private void WriteDebug(String a, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(a.toString()), new Text());
	}
	*/
}
