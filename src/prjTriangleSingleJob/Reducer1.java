package prjTriangleSingleJob;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.javatuples.Pair;
import org.javatuples.Triplet;

public class Reducer1 extends
		Reducer<LongLongLongLong, LongWritable, Text, Text> {

	private Map<Pair<Long, Long>, List<Triplet<Long, Long, Long>>> partialJoin = new HashMap<Pair<Long, Long>, List<Triplet<Long, Long, Long>>>();
	private Text outText = new Text();

	@Override
	protected void reduce(LongLongLongLong key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		//partialJoin.clear();
		List<Long> tmpList = new LinkedList<Long>();

		for (LongWritable valText : values) {
			Long from = key.getfourth().get();
			Long to = valText.get();
			Pair<Long, Long> p = CreatePair(to, from);
			if (partialJoin.containsKey(p)	&& key.getRel().toString().equals("C")) {
				WriteContext(partialJoin.get(p), context);
				partialJoin.remove(p);
			}
			for (Long tmpTo : tmpList) {
				Pair<Long, Long> l = CreatePair(to, tmpTo);
				if (!to.equals(tmpTo) && key.getRel().toString().equals("B")) {
					Triplet<Long, Long, Long> t  = new Triplet<Long, Long, Long>(from, l.getValue0(),l.getValue1());
					if(partialJoin.containsKey(l)){
						if(!partialJoin.get(l).contains(t))
							partialJoin.get(l).add(t);
					}
					else{
						List<Triplet<Long, Long, Long>> lt= new LinkedList<Triplet<Long, Long, Long>>();
						lt.add(new Triplet<Long, Long, Long>(from, l.getValue0(),l.getValue1()));						
						partialJoin.put(l,lt);						
					}
						
				}
			}
			if (!tmpList.contains(to)&& key.getRel().toString().equals("A")) {
				tmpList.add(to);
			}
		}
	}

	private Pair<Long, Long> CreatePair(Long to, Long tmpTo) {
		if (tmpTo < to)
			return new Pair<Long, Long>(tmpTo, to);
		return new Pair<Long, Long>(to, tmpTo);
	}

	private void WriteContext(List<Triplet<Long, Long, Long>> val, Context context)
			throws IOException, InterruptedException {
		for(Triplet<Long, Long, Long> t:val)
		context.write(new Text(t.toString()), new Text());
	}
}
