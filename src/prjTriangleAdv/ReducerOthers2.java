package prjTriangleAdv;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ReducerOthers2 extends
		Reducer<KeyClosure, IntWritable, Text, Text> {
	private static final Log _log = LogFactory.getLog(ReducerOthers2.class);

	@Override
	protected void reduce(KeyClosure key, Iterable<IntWritable> values,
						  Context context) throws IOException, InterruptedException {

		Iterator<IntWritable> iteratorA = values.iterator();
		if(iteratorA.hasNext())
		{
			IntWritable firstValue=iteratorA.next();
			WriteContextDebug("#I",key.getBucketIndex(),firstValue.get(),key.getB(),key.getC(),context);
			//è un arco do chiusura
			if(firstValue.get()<0){
				while(iteratorA.hasNext()) {
					IntWritable a=iteratorA.next();
					WriteContextDebug("#W",key.getBucketIndex(),a.get(),key.getB(),key.getC(),context);
					if(a.get()>0)
						WriteContext(a.get(),key.getB(),key.getC(),context);
				}
			}

		}
	}

	private void WriteContext(Integer a, Integer b, Integer c, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(a.toString()+ "\t"+b.toString()+ "\t"+c.toString()+ "\t"), new Text());
	}
	private void WriteContextDebug(String tag,Integer bucketIndex , Integer a, Integer b,Integer c, Context context)
			throws IOException, InterruptedException {
		//context.write(new Text(tag +"\t"+ bucketIndex.toString()+ "\t"+a.toString()+ "\t"+b.toString()+ "\t"+c.toString() ), new Text());
	}

}
