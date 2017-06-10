package prjTriangleAdv;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.*;

public class ReducerOthers extends
		Reducer<BucketItem, BucketItem, Text, Text> {
	private static final Log _log = LogFactory.getLog(ReducerOthers.class);

	private Integer fromOld;
	private HashSet<Pair<Integer,Integer>> tmpAList = new HashSet<Pair<Integer,Integer>>();
	//private List<Integer> tmpBListTmp = new ArrayList<Integer>();
	private Map<Pair<Integer, Integer>,List<Integer>> tmpBList = new HashMap<Pair<Integer, Integer>, List<Integer>>();

	private void Init(int from){
		tmpAList = new HashSet<Pair<Integer,Integer>>();
		fromOld=from;
	}
	private void cleanUp(int from){

		for(Iterator<Map.Entry<Pair<Integer,Integer>,List<Integer>>> it = tmpBList.entrySet().iterator(); it.hasNext(); ) {
			Map.Entry<Pair<Integer,Integer>,List<Integer>> entry = it.next();
			Pair<Integer,Integer> k=entry.getKey();
			if(from==k.getValue0()) {
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
	@Override
	protected void reduce(BucketItem bucketItem, Iterable<BucketItem> values,
						  Context context) throws IOException, InterruptedException {

		Iterator<BucketItem> it = values.iterator();

		//WriteDebug("\t NEW",context);
		while(it.hasNext()) {
			String typeRel = bucketItem.getTypeRel().toString();
			int from =bucketItem.getFrom();
			int fromDegree =bucketItem.getFromDegree();
			BucketItem toW=it.next();
			int to= toW.getFrom();
			int toDegree =toW.getFromDegree();

			if(typeRel.equals("A")){
				if(fromOld==null || !fromOld.equals(from)) {
					Init(from);
				}
				Pair<Integer,Integer> p=new Pair<Integer, Integer>(to, toDegree);
				if(!tmpAList.contains(p)){
					tmpAList.add(p);
				}
			}
			//WriteDebug(new Integer(from).toString()+"\t"+new Integer(to).toString()+"\t"+typeRel+new Integer(bucketItem.getBucketIndex().get()).toString() ,context);

			if(typeRel.equals("B")){

				if(fromOld !=null &&fromOld.equals(from)){
					for (Pair<Integer,Integer> vPB : tmpAList) {
						if(vPB.getValue1()<toDegree || (vPB.getValue1()==toDegree && vPB.getValue0()<to )){
							Pair<Integer,Integer> pair= new Pair<Integer, Integer>(vPB.getValue0(),to);
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
				tmpAList= new HashSet<Pair<Integer, Integer>>();
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
			//cleanUp(from);
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
