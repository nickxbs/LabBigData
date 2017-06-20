package prjTriangleAdv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MapperDegreeOthers1 extends
        Mapper<LongWritable, Text, BucketItemDegree, BucketItemDegree> {
    private BucketItemDegree toW = new BucketItemDegree();
    private int buckets;
    private int count = 0;
    private double sqrt = 0;
    private Map<Integer, Integer> degreeMap = new HashMap<Integer, Integer>();

    Path[] cachefiles = new Path[0]; //To store the path of lookup files

    /********************Setup Method******************************************/
    @Override
    public void setup(Context context)

    {
        Configuration conf = context.getConfiguration();

        try {

            cachefiles = DistributedCache.getLocalCacheFiles(conf);
            BufferedReader readerCount = new BufferedReader(new FileReader(cachefiles[0].toString()));
            BufferedReader readerDegree = new BufferedReader(new FileReader(cachefiles[1].toString()));
            String line;

            while ((line = readerCount.readLine()) != null) {
                if (line.startsWith("COUNT")) {
                    line = line.replaceAll("^\\s+", "");
                    String[] sp = line.split("\\s+");// splits on TAB
                    int localCount = Integer.parseInt(sp[1]);
                    count += localCount;
                    sqrt = Math.sqrt(count);
                }
            }
            while ((line = readerDegree.readLine()) != null) {
                if (line.startsWith("DEGREE")) {
                    line = line.replaceAll("^\\s+", "");
                    String[] sp = line.split("\\s+");// splits on TAB
                    int node = Integer.parseInt(sp[1]);
                    int degree = Integer.parseInt(sp[2]);
                    if (!degreeMap.containsKey(node)) {
                        degreeMap.put(node, degree);
                    }
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        this.buckets = conf.getInt("b", 2);

        String line = value.toString();

        if (!line.startsWith("#") && !line.startsWith("COUNT") && !line.startsWith("DEGREE")) {
            line = line.replaceAll("^\\s+", "");
            String[] sp = line.split("\\s+");// splits on TAB
            int lp0 = Integer.parseInt(sp[0]);
            int lp1 = Integer.parseInt(sp[1]);
            if (degreeMap.containsKey(lp0) && degreeMap.containsKey(lp1)) {
                int dg0 = degreeMap.get(lp0);
                int dg1 = degreeMap.get(lp1);

                //questo emit lo faccio solo se sono entrambi nodi HeavyHitter degree > sqrt(count)
                if (lp0 != lp1) {
                    if (dg0 < dg1 || (dg0 == dg1 && lp0 < lp1)) {
                        SetContext(context, lp0, dg0, lp1, dg1);
                    } else {
                        SetContext(context, lp1, dg1, lp0, dg0);
                    }
                }
            }else {
                throw new InterruptedException("Error: " + line + " degreeMap:" + degreeMap.size() + " count:" + count);
            }
        }
    }

    private void SetContext(Context context, int from, int fromDegree, int to, int toDegree)
            throws IOException, InterruptedException {
        toW = new BucketItemDegree("", -1, to, toDegree);
        for (int j = 0; j < buckets; j++) {
            if (fromDegree < sqrt) {
                int aIndex = (int) (((Math.pow(buckets, 2)) * (from % buckets)) + (buckets * (to % buckets)) + j);
                context.write(new BucketItemDegree("A", aIndex, from, fromDegree), toW);
                int bIndex = (int) (((Math.pow(buckets, 2)) * (from % buckets)) + (buckets * (j)) + (to % buckets));
                context.write(new BucketItemDegree("B", bIndex, from, fromDegree), toW);
            }
            int cIndex = (int) (((Math.pow(buckets, 2)) * (j)) + (buckets * (from % buckets)) + (to % buckets));
            context.write(new BucketItemDegree("C", cIndex, from, fromDegree), toW);
        }
    }

}
