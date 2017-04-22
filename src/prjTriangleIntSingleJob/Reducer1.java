package prjTriangleIntSingleJob;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.io.IOException;
import java.util.*;

public class Reducer1 extends
		Reducer<BucketItem, IntWritable, Text, Text> {

	private Map<Pair<Integer, Integer>, List<Triplet<Integer, Integer, Integer>>> partialJoin = new HashMap<Pair<Integer, Integer>, List<Triplet<Integer, Integer, Integer>>>();

	private int fromOld;
	private List<Integer> tmpAList = new LinkedList<Integer>();
	private Map<Pair<Integer, Integer>,Integer> tmpBList = new HashMap<Pair<Integer, Integer>, Integer>();

	private void Init(int from){
		tmpAList = new LinkedList<Integer>();
		fromOld=from;
	}

	@Override
	protected void reduce(BucketItem key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		//partialJoin.clear();
		String typeRel = key.getTypeRel().toString();
		if(typeRel == "A"){
			Init(key.getFrom().get());
			for (IntWritable valText : values) {
				tmpAList.add(valText.get());
			}
		}
		if(typeRel == "B"){
			if(fromOld!=key.getFrom().get()){
				Init(key.getFrom().get());
			}else{
				for (IntWritable valC : values) {
					for (int vB : tmpAList) {
						int vC=valC.get();
						if(vB<vC){
							tmpBList.put(new Pair<Integer, Integer>(vB,vC), key.getFrom().get());
						}
					}
				}
			}
		}
		if(typeRel == "C"){
			int vB=key.getFrom().get();
			for (IntWritable valC : values) {
				int vC=valC.get();
				Pair<Integer,Integer> pair= new Pair<Integer, Integer>(vB,vC);
				if(tmpBList.containsKey(pair)){
					int vA = tmpBList.get(pair);
					//ok trovato triangoloù
					WriteContext(vA,vB,vC,context);
				}

			}
			for(Iterator<Map.Entry<Pair<Integer,Integer>,Integer>> it = tmpBList.entrySet().iterator(); it.hasNext(); ) {
				Map.Entry<Pair<Integer,Integer>,Integer> entry = it.next();
				Pair<Integer,Integer> k=entry.getKey();
				if(vB>k.getValue0()) {
					it.remove();
				}
			}
		}
	}

	private Pair<Integer, Integer> CreatePair(int to, int tmpTo) {
		if (tmpTo < to) {
			return new Pair<Integer, Integer>(tmpTo, to);
		}
		return new Pair<Integer, Integer>(to, tmpTo);
	}

	private void WriteContext(Integer a, Integer b, Integer c, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(a.toString()+ "\t"+b.toString()+ "\t"+c.toString()+ "\t"), new Text());
	}
}
