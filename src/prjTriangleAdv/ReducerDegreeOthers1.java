package prjTriangleAdv;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

public class ReducerDegreeOthers1 extends
		Reducer<BucketItemDegree, BucketItemDegree, Text, Text> {
	private static final Log _log = LogFactory.getLog(ReducerDegreeOthers1.class);

	private Integer priviusFrom;
	private HashSet<Pair<Integer,Integer>> listToWithDegree = new HashSet<Pair<Integer,Integer>>();
	//private List<Integer> mapPairTo_From = new ArrayList<Integer>();

	private void Init(int from){
		listToWithDegree.clear();
		priviusFrom =from;
	}

	@Override
	protected void reduce(BucketItemDegree bucketKey, Iterable<BucketItemDegree> values,
						  Context context) throws IOException, InterruptedException {

		Iterator<BucketItemDegree> toBuckets = values.iterator();
		WriteContextDebug("#Init",bucketKey.getBucketIndex(),bucketKey.getFrom(),-1,new Text(),context);
		while(toBuckets.hasNext()) {
			BucketItemDegree toBucket=toBuckets.next();
			WriteContextDebug("#while",bucketKey.getBucketIndex(),bucketKey.getFrom(),toBucket.getFrom(),bucketKey.getTypeRel(),context);
			String typeRel = bucketKey.getTypeRel().toString();
			int from =bucketKey.getFrom();
			int bucketIndex =bucketKey.getBucketIndex();
			int to= toBucket.getFrom();
			int toDegree =toBucket.getFromDegree();


			if(typeRel.equals("A")){
				if(priviusFrom ==null || !priviusFrom.equals(from)) {
					Init(from);
				}
				Pair<Integer,Integer> p=new Pair<Integer, Integer>(to, toDegree);
				if(!listToWithDegree.contains(p)){
					listToWithDegree.add(p);
				}
			}
			if(typeRel.equals("B")){
				// B ha senso solo se per lo stesso from ho avuto una relazione A
				if(priviusFrom !=null && priviusFrom.equals(from)){
					for (Pair<Integer,Integer> toWithDegree : listToWithDegree) {
						if(toWithDegree.getValue1()<toDegree || (toWithDegree.getValue1()==toDegree && toWithDegree.getValue0()!=to )){
							WriteContext(bucketIndex,toWithDegree.getValue0(),to,from,context);
						}
					}
				} else{
					listToWithDegree.clear();
					listToWithDegree=new HashSet<Pair<Integer, Integer>>();
				}
			}
			if(typeRel.equals("C") && priviusFrom !=null ){
				listToWithDegree.clear();
				listToWithDegree=new HashSet<Pair<Integer,Integer>>();
				int vB=from;
				int vC=to;
				WriteContext(bucketIndex,vB,vC,-1,context);
			}
		}
	}

	private void WriteContext(Integer bucketIndex , Integer b, Integer c,Integer a, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(bucketIndex.toString()+ "\t"+b.toString()+ "\t"+c.toString()+ "\t"+a.toString() ), new Text());
	}
	private void WriteContextDebug(String tag,Integer bucketIndex , Integer from, Integer to,Text type, Context context)
			throws IOException, InterruptedException {
		//context.write(new Text(tag +"\t"+ bucketIndex.toString()+ "\t"+from.toString()+ "\t"+to.toString()+ "\t"+type.toString() ), new Text());
	}

}
