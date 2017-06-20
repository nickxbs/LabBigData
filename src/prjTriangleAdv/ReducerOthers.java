package prjTriangleAdv;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.*;

public class ReducerOthers extends
		Reducer<BucketItem, BucketItem, Text, Text> {
	private static final Log _log = LogFactory.getLog(ReducerOthers.class);

	private Long priviusFrom;
	private HashSet<Pair<Long,Long>> listToWithDegree = new HashSet<Pair<Long,Long>>();
	//private List<Long> mapPairTo_From = new ArrayList<Long>();
	private Map<Pair<Long, Long>,List<Long>> mapPairTo_From = new HashMap<Pair<Long, Long>, List<Long>>();

	private void Init(long from){
		listToWithDegree = new HashSet<Pair<Long,Long>>();
		priviusFrom =from;
	}
	private void cleanUp(int from){

		for(Iterator<Map.Entry<Pair<Long,Long>,List<Long>>> it = mapPairTo_From.entrySet().iterator(); it.hasNext(); ) {
			Map.Entry<Pair<Long,Long>,List<Long>> entry = it.next();
			Pair<Long,Long> k=entry.getKey();
			if(from==k.getValue0()) {
				it.remove();
			}
		}

		/*
		for(Iterator<Long> it = mapPairTo_From.iterator(); it.hasNext(); ) {
			Long entry = it.next();
			if(from>entry) {
				it.remove();
			}
		}*/
	}
	@Override
	protected void reduce(BucketItem bucketKey, Iterable<BucketItem> values,
						  Context context) throws IOException, InterruptedException {

		Iterator<BucketItem> toBuckets = values.iterator();

		//WriteDebug("\t NEW",context);
		while(toBuckets.hasNext()) {
			String typeRel = bucketKey.getTypeRel().toString();
			long from =bucketKey.getFrom();

			BucketItem toBucket=toBuckets.next();
			long to= toBucket.getFrom();
			long toDegree =toBucket.getFromDegree();

			if(typeRel.equals("A")){
				if(priviusFrom ==null || !priviusFrom.equals(from)) {
					Init(from);
				}
				Pair<Long,Long> p=new Pair<Long, Long>(to, toDegree);
				if(!listToWithDegree.contains(p)){
					listToWithDegree.add(p);
				}
			}
			//WriteDebug(new Long(from).toString()+"\t"+new Long(to).toString()+"\t"+typeRel+new Long(bucketItem.getBucketIndex().get()).toString() ,context);

			if(typeRel.equals("B")){
				// B ha senso solo se per lo stesso from ho avuto una relazione A
				if(priviusFrom !=null && priviusFrom.equals(from)){
					for (Pair<Long,Long> toWithDegree : listToWithDegree) {
						if(toWithDegree.getValue1()<toDegree || (toWithDegree.getValue1()==toDegree && toWithDegree.getValue0()!=to )){
							Pair<Long,Long> pairTo= new Pair<Long, Long>(toWithDegree.getValue0(),to);
							if(!mapPairTo_From.containsKey(pairTo)){
								List<Long> listFrom= new LinkedList<Long>();
								listFrom.add(from);
								mapPairTo_From.put(pairTo, listFrom);
							} else{
								List<Long> listFrom= mapPairTo_From.get(pairTo);
								if(!listFrom.contains(from))
									listFrom.add(from);
							}
						}
					}
				}
			}


			if(typeRel.equals("C") && priviusFrom !=null ){
				listToWithDegree = new HashSet<Pair<Long, Long>>();
				//WriteContextStr("mapPairTo_From: "+new Long(mapPairTo_From.size()).toString(),context);
				//cleanUp(from);

				long vB=from;
				long vC=to;
				Pair<Long,Long> pair= new Pair<Long, Long>(vB,vC);
				if(mapPairTo_From.containsKey(pair)){
					List<Long> listvA = mapPairTo_From.get(pair);
					mapPairTo_From.remove(pair);
					for (long vA : listvA) {
						//ok trovato triangolo
						WriteContext(vA,vB,vC,context);
					}
				}
				//cleanUp(from);

			}
			//cleanUp(from);
		}
	}

	private void WriteContext(Long a, Long b, Long c, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(a.toString()+ "\t"+b.toString()+ "\t"+c.toString()+ "\t"), new Text());
	}
	private void WriteDebug(String a, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(a.toString()), new Text());
	}
}
