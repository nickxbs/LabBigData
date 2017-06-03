package prjTriangleNumericUndir;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<LongLongBit, LongWritable, Text, Text> {
    private Text outText = new Text();
    //private List<Long> vals = new LinkedList<Long>();

    @Override
    protected void reduce(LongLongBit key, Iterable<LongWritable> values,
                          Context context) throws IOException, InterruptedException {
        boolean ok = false;
        //vals.clear();
        Boolean k = key.getTer().get();
        if (!k) {
            for (LongWritable val : values) {
                if (val.get() >0) // && val>0
                    WriteContext(val.get(), key.getFirst().get(), key.getSecond().get(), context);
            }
        }
    }

    private void WriteContext(Long first, Long second, Long ter,
                              Context context) throws IOException, InterruptedException {
        // per evitare duplicazioni

        outText.set(first + " " + second + " " + ter);
        context.write(outText, new Text());
        //vals.add(second);
        return;

    }
}