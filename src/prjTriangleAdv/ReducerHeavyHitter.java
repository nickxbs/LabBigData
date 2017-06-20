package prjTriangleAdv;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.*;

public class ReducerHeavyHitter extends
        Reducer<BucketItemDegree, BucketItemDegree, Text, Text> {
    private static final Log _log = LogFactory.getLog(ReducerHeavyHitter.class);

    private Integer priviusFrom;
    private HashSet<Pair<Integer,Integer>> listToWithDegree = new HashSet<Pair<Integer,Integer>>();
    //private List<Integer> mapPairTo_From = new ArrayList<Integer>();
    private Map<Pair<Integer, Integer>,List<Integer>> mapPairTo_From = new HashMap<Pair<Integer, Integer>, List<Integer>>();

    private void Init(int from){
        listToWithDegree = new HashSet<Pair<Integer,Integer>>();
        priviusFrom =from;
    }

    @Override
    protected void reduce(BucketItemDegree bucketKey, Iterable<BucketItemDegree> values,
                          Context context) throws IOException, InterruptedException {

        Iterator<BucketItemDegree> toBuckets = values.iterator();

        //WriteDebug("\t NEW",context);
        while(toBuckets.hasNext()) {
            String typeRel = bucketKey.getTypeRel().toString();
            int from =bucketKey.getFrom();
            int fromDegree =bucketKey.getFromDegree();
            BucketItemDegree toBucket=toBuckets.next();
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
            //WriteDebug(new Integer(from).toString()+"\t"+new Integer(to).toString()+"\t"+typeRel+new Integer(bucketItem.getBucketIndex().get()).toString() ,context);

            if(typeRel.equals("B")){
                // B ha senso solo se per lo stesso from ho avuto una relazione A
                if(priviusFrom !=null && priviusFrom.equals(from)){
                    for (Pair<Integer,Integer> toWithDegree : listToWithDegree) {
                        if(toWithDegree.getValue1()<toDegree || (toWithDegree.getValue1()==toDegree && toWithDegree.getValue0()<to )){
                            Pair<Integer,Integer> pairTo= new Pair<Integer, Integer>(toWithDegree.getValue0(),to);
                            if(!mapPairTo_From.containsKey(pairTo)){
                                List<Integer> listFrom= new LinkedList<Integer>();
                                listFrom.add(from);
                                mapPairTo_From.put(pairTo, listFrom);
                            } else{
                                List<Integer> listFrom= mapPairTo_From.get(pairTo);
                                if(!listFrom.contains(from))
                                    listFrom.add(from);
                            }
                        }
                    }
                }
            }


            if(typeRel.equals("C") && priviusFrom !=null ){
                listToWithDegree = new HashSet<Pair<Integer, Integer>>();
                //WriteContextStr("mapPairTo_From: "+new Integer(mapPairTo_From.size()).toString(),context);
                //cleanUp(from);

                int vB=from;
                int vC=to;
                Pair<Integer,Integer> pair= new Pair<Integer, Integer>(vB,vC);
                if(mapPairTo_From.containsKey(pair)){
                    List<Integer> listvA = mapPairTo_From.get(pair);
                    mapPairTo_From.remove(pair);
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
        context.write(new Text(a.toString() + "\t" + b.toString() + "\t" + c.toString() + "\t"), new Text());
    }

    private void WriteDebug(String a, Context context)
            throws IOException, InterruptedException {
        context.write(new Text(a.toString()), new Text());
    }
}
