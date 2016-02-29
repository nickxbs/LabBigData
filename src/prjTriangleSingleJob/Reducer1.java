package prjTriangleSingleJob;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.javatuples.Pair;
import org.javatuples.Triplet;

public class Reducer1 extends
		Reducer<LongLongLongLong, IntWritable, Text, Text> {

	private Map<String, List<String>> partialJoin = new HashMap<String, List<String>>();
	private Text outText = new Text();
	private static final Log LOG = LogFactory.getLog(Mapper1.class);


	@Override
	protected void reduce(LongLongLongLong key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		//partialJoin.clear();
		List<Integer> tmpList = new LinkedList<Integer>();
		LOG.info("My message");

		for (IntWritable valText : values) {
			Integer from = key.getfourth().get();
			Integer to = valText.get();
			Integer[] p = CreatePair(to, from);
			if (partialJoin.containsKey(p[0].toString()+"|"+p[1].toString())	&& key.getRel().toString().equals("C")) {
				WriteContext(partialJoin.get(p[0].toString()+"|"+p[1].toString()), context);
				partialJoin.remove(p[0].toString()+"|"+p[1].toString());
			}
			for (Integer tmpTo : tmpList) {
				Integer[] l = CreatePair(to, tmpTo);
				if (!to.equals(tmpTo) && key.getRel().toString().equals("B")) {
					Integer[] t  = new Integer[]{from, l[0],l[1]};
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
		}
	}

	private Integer[] CreatePair(Integer to, Integer tmpTo) {
		if (tmpTo < to)
			return new Integer[]{tmpTo, to};
		return new Integer[]{to, tmpTo};
	}

	private void WriteContext(List<String> val, Context context)
			throws IOException, InterruptedException {
		for(String t:val)
		context.write(new Text(t), new Text());
	}
}
