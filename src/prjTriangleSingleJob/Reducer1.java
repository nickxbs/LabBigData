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

<<<<<<< HEAD
	private Map<Pair<Long, Long>, List<Triplet<Long, Long, Long>>> partialJoin = new HashMap<Pair<Long, Long>, List<Triplet<Long, Long, Long>>>();
=======
	private Map<String, List<String>> partialJoin = new HashMap<String, List<String>>();
	private Text outText = new Text();

>>>>>>> 6b2c26c26c809723d0eef73ae4b7dfe7ae6c25d3
	@Override
	protected void reduce(LongLongLongLong key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		//partialJoin.clear();
		List<Long> tmpList = new LinkedList<Long>();

		for (LongWritable valText : values) {
			Long from = key.getfourth().get();
			Long to = valText.get();
			Long[] p = CreatePair(to, from);
			if (partialJoin.containsKey(p[0].toString()+"|"+p[1].toString())	&& key.getRel().toString().equals("C")) {
				WriteContext(partialJoin.get(p[0].toString()+"|"+p[1].toString()), context);
				partialJoin.remove(p[0].toString()+"|"+p[1].toString());
			}
			for (Long tmpTo : tmpList) {
				Long[] l = CreatePair(to, tmpTo);
				if (!to.equals(tmpTo) && key.getRel().toString().equals("B")) {
					Long[] t  = new Long[]{from, l[0],l[1]};
					if(partialJoin.containsKey(l[0].toString()+"|"+l[1].toString())){
						if(!partialJoin.get(l[0].toString()+"|"+l[1].toString()).contains(t[0].toString()+"|"+t[1].toString()+"|"+t[2].toString()))
							partialJoin.get(l[0].toString()+"|"+l[1].toString()).add(t[0].toString()+"|"+t[1].toString()+"|"+t[2].toString());
					}
					else{
						List<String> lt= new LinkedList<String>();
						lt.add(from+"|"+l[0]+"|"+l[1]);						
						partialJoin.put(l[0].toString()+"|"+l[1].toString(),lt);						
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

	private Long[] CreatePair(Long to, Long tmpTo) {
		if (tmpTo < to)
			return new Long[]{tmpTo, to};
		return new Long[]{to, tmpTo};
	}

	private void WriteContext(List<String> val, Context context)
			throws IOException, InterruptedException {
		for(String t:val)
		context.write(new Text(t), new Text());
	}
}
