package prjTriangleAdv;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ReducerCount extends
        Reducer<IntWritable, Text, Text, Text> {
    private static final Log _log = LogFactory.getLog(ReducerCount.class);

    private int count = 0;

    @Override
    protected void reduce(IntWritable nodeW, Iterable<Text> values,
                          Context context) throws IOException, InterruptedException {

        Iterator<Text> it = values.iterator();

        while (it.hasNext()) {
            it.next();
            count++;
        }
        WriteDegree(count,context);
    }

    private void WriteDegree(int count, Context context)
            throws IOException, InterruptedException {
        context.write(new Text("COUNT\t"+ new Integer(count) + "\t"), new Text());
    }
}
