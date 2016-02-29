package prjTriangleIntSingleJob;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.javatuples.Pair;
import org.javatuples.Triplet;

public class Reducer1 extends
		Reducer<BucketItem, IntWritable, Text, Text> {

	private Map<Pair<Integer, Integer>, List<Triplet<Integer, Integer, Integer>>> partialJoin = new HashMap<Pair<Integer, Integer>, List<Triplet<Integer, Integer, Integer>>>();
	@Override
	protected void reduce(BucketItem key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		//partialJoin.clear();
		List<Integer> tmpList = new LinkedList<Integer>();

		for (IntWritable valText : values) {
			int from = key.getfourth().get();
			int to = valText.get();
			Pair<Integer, Integer> p = CreatePair(to, from);
			if (partialJoin.containsKey(p)	&& key.getRel().toString().equals("C")) {
				WriteContext(partialJoin.get(p), context);
				partialJoin.remove(p);
			}
			for (int tmpTo : tmpList) {
				Pair<Integer, Integer> l = CreatePair(to, tmpTo);
				if (to!=tmpTo && key.getRel().toString().equals("B")) {
					Triplet<Integer, Integer, Integer> t  = new Triplet<Integer, Integer, Integer>(from, l.getValue0(),l.getValue1());
					if(partialJoin.containsKey(l)){
						if(!partialJoin.get(l).contains(t))
							partialJoin.get(l).add(t);
					}
					else{
						List<Triplet<Integer, Integer, Integer>> lt= new LinkedList<Triplet<Integer, Integer, Integer>>();
						lt.add(new Triplet<Integer, Integer, Integer>(from, l.getValue0(),l.getValue1()));						
						partialJoin.put(l,lt);						
					}
						
				}
			}
			if (!tmpList.contains(to)&& key.getRel().toString().equals("A")) {
				tmpList.add(to);
			}
			System.out.println("tmpList "+tmpList.size());
			System.out.println("partialJoin "+partialJoin.size());
		}
	}

	private Pair<Integer, Integer> CreatePair(int to, int tmpTo) {
		if (tmpTo < to)
			return new Pair<Integer, Integer>(tmpTo, to);
		return new Pair<Integer, Integer>(to, tmpTo);
	}

	private void WriteContext(List<Triplet<Integer, Integer, Integer>> val, Context context)
			throws IOException, InterruptedException {
		for(Triplet<Integer, Integer, Integer> t:val)
		context.write(new Text(t.toString()), new Text());
	}
}
