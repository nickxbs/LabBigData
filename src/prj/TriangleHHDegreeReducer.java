package prj;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


	public class TriangleHHDegreeReducer extends
	Reducer<LongWritable,LongWritablePair, LongWritable, Text> {
		public void reduce(LongWritable key, Iterable<LongWritablePair> values, Context context)
				throws IOException, InterruptedException {

			List<Long> list = new ArrayList<Long>();
			for (LongWritablePair val : values) {
				if(!list.contains(val.getSecond()))
					list.add(val.getSecond().get());
			}
			for(int i=0;i<list.size();i++)
			{
				Long iFirst=list.get(i);
						
				for(int j=i+1;j<list.size();j++)
				{
					Long iSecond=list.get(j);
					context.write(key, new Text(":"+iFirst+" "+iSecond));
				}
			}
		}
	}