package prjTriangleAdv;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.*;

public class ReducerOthers1 extends
		Reducer<BucketItem, BucketItem, Text, Text> {
	private static final Log _log = LogFactory.getLog(ReducerOthers1.class);

	private Integer priviusFrom;
	private HashSet<Integer> listToWithDegree = new HashSet<Integer>();
	//private List<Integer> mapPairTo_From = new ArrayList<Integer>();

	private void Init(int from){
		listToWithDegree.clear();
		priviusFrom =from;
	}

	@Override
	protected void reduce(BucketItem bucketKey, Iterable<BucketItem> values,
						  Context context) throws IOException, InterruptedException {

		Iterator<BucketItem> toBuckets = values.iterator();
		WriteContextDebug("#Init",bucketKey.getBucketIndex(),bucketKey.getFrom(),-1,new Text(),context);
		while(toBuckets.hasNext()) {
			BucketItem toBucket=toBuckets.next();
			WriteContextDebug("#while",bucketKey.getBucketIndex(),bucketKey.getFrom(),toBucket.getFrom(),bucketKey.getTypeRel(),context);
			String typeRel = bucketKey.getTypeRel().toString();
			int from =bucketKey.getFrom();
			int bucketIndex =bucketKey.getBucketIndex();
			int to= toBucket.getFrom();


			if(typeRel.equals("A")){
				if(priviusFrom ==null || !priviusFrom.equals(from)) {
					Init(from);
				}
				Integer p=new Integer(to);
				if(!listToWithDegree.contains(p)){
					listToWithDegree.add(p);
				}
			}
			if(typeRel.equals("B")){
				// B ha senso solo se per lo stesso from ho avuto una relazione A
				if(priviusFrom !=null && priviusFrom.equals(from)){
					for (Integer toWithDegree : listToWithDegree) {
						if(toWithDegree<to ){
							WriteContext(bucketIndex,toWithDegree,to,from,context);
						}
					}
				} else{
					listToWithDegree.clear();
					listToWithDegree=new HashSet<Integer>();
				}
			}
			if(typeRel.equals("C") && priviusFrom !=null ){
				listToWithDegree.clear();
				listToWithDegree=new HashSet<Integer>();
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
