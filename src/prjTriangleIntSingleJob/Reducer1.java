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


	private Integer fromOld;
	private HashSet<Integer> tmpAList = new HashSet<Integer>();
	//private List<Integer> tmpBListTmp = new ArrayList<Integer>();
	private Map<Pair<Integer, Integer>,List<Integer>> tmpBList = new HashMap<Pair<Integer, Integer>, List<Integer>>();

	private void Init(int from){
		tmpAList = new HashSet<Integer>();
		fromOld=from;
	}
	private void cleanUp(int from){

		for(Iterator<Map.Entry<Pair<Integer,Integer>,List<Integer>>> it = tmpBList.entrySet().iterator(); it.hasNext(); ) {
			Map.Entry<Pair<Integer,Integer>,List<Integer>> entry = it.next();
			Pair<Integer,Integer> k=entry.getKey();
			if(from>k.getValue0()) {
				it.remove();
			}
		}

		/*
		for(Iterator<Integer> it = tmpBListTmp.iterator(); it.hasNext(); ) {
			Integer entry = it.next();
			if(from>entry) {
				it.remove();
			}
		}*/
	}
	private int maxSize=0;

	@Override
	protected void reduce(BucketItem key, Iterable<IntWritable> values,
						  Context context) throws IOException, InterruptedException {
		String typeRel = key.getTypeRel().toString();
		for (IntWritable valText : values) {
			int from =key.getFrom().get();
			int to= valText.get();

			if(typeRel.equals("A")){
				if(fromOld==null || !fromOld.equals(from)) {
					Init(from);
				}
				if(!tmpAList.contains(to)){
					tmpAList.add(to);
				}
			}
			//WriteContextStr(new Integer(from).toString()+"\t"+new Integer(to).toString()+"\t"+typeRel+new Integer(key.getBucketIndex().get()).toString() ,context);

			if(typeRel.equals("B")){

				if(fromOld !=null &&fromOld.equals(from)){
					for (int vB : tmpAList) {
						int vC=to;
						Pair<Integer,Integer> pair= new Pair<Integer, Integer>(vB,vC);
						if(vB<vC){
							if(!tmpBList.containsKey(pair)){
								List<Integer> listFrom= new LinkedList<Integer>();
								listFrom.add(from);
								tmpBList.put(pair, listFrom);
							} else{
								List<Integer> listFrom= tmpBList.get(pair);
								if(!listFrom.contains(from))
									listFrom.add(from);
							}
						}
					}
				}
			}


			if(typeRel.equals("C") && fromOld !=null ){
				tmpAList= new HashSet<Integer>();
				//WriteContextStr("tmpBList: "+new Integer(tmpBList.size()).toString(),context);
				//cleanUp(from);

				int vB=from;
				int vC=to;
				Pair<Integer,Integer> pair= new Pair<Integer, Integer>(vB,vC);
				if(tmpBList.containsKey(pair)){
						List<Integer> listvA = tmpBList.get(pair);
						tmpBList.remove(pair);
						for (int vA : listvA) {
							//ok trovato triangolo
							WriteContext(vA,vB,vC,context);
						}
				}
				//cleanUp(from);

			}

		}
	}

	private void WriteContext(Integer a, Integer b, Integer c, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(a.toString()+ "\t"+b.toString()+ "\t"+c.toString()+ "\t"), new Text());
	}
	private void WriteContextStr(String a, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(a.toString()), new Text());
	}
}
