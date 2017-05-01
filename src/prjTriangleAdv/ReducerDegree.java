package prjTriangleAdv;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.*;

public class ReducerDegree extends
        Reducer<IntWritable, Text, Text, Text> {
    private static final Log _log = LogFactory.getLog(ReducerDegree.class);

    private int oldNode = -1;
    private int deg = 0;

    @Override
    protected void reduce(IntWritable nodeW, Iterable<Text> values,
                          Context context) throws IOException, InterruptedException {

        Iterator<Text> it = values.iterator();

        while (it.hasNext()) {
            it.next();
            int node = nodeW.get();
            if (oldNode >= 0 && node == oldNode) {
                deg++;
            } else {
                oldNode = node;
                deg = 1;
            }
        }
        WriteDegree(oldNode, deg, context);
        deg = 0;
        oldNode = -1;
    }

    private void WriteDegree(Integer node, Integer degree, Context context)
            throws IOException, InterruptedException {
        context.write(new Text("DEGREE\t" + node.toString() + "\t" + degree.toString() + "\t"), new Text());
    }
}
