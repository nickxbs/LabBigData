package prjTriangleNumericUndir;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<LongWritable, LongWritable, LongLong, LongWritable> {

    private List<Long> partialJoin = new LinkedList<Long>();
    private LongLong outText = new LongLong();

    @Override
    protected void reduce(LongWritable fromA, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        partialJoin.clear();

        for (LongWritable toWritable : values) {
            Long to = toWritable.get();
            if (!partialJoin.contains(to))
                partialJoin.add(to);
        }
        for (Long toB : partialJoin) {
            WriteContext(fromA, toB, context);
        }


    }

    private void WriteContext(LongWritable fromA, Long toB, Context context)
            throws IOException, InterruptedException {
        for (Long toC : partialJoin) {
            if (toB < toC) {
                outText.set(toB, toC);
                context.write(outText, fromA);
            }
        }
    }
}