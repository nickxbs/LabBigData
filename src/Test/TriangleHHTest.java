package Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import prj.LongWritablePair;
import prj.TriangleHHDegreeMapper;
import prj.TriangleHHDegreeReducer;


public class TriangleHHTest extends TestCase {

	 	MapDriver<LongWritable, Text, LongWritable, LongWritablePair> mapDriver;
	 	ReduceDriver<LongWritable, LongWritablePair, LongWritable, Text> reduceDriver;
	 	MapReduceDriver<LongWritable, Text, LongWritable, LongWritablePair, LongWritable, Text> mapreduceDriver;
	 
	  @Before
	  public void setUp() {
	    TriangleHHDegreeMapper mapper = new TriangleHHDegreeMapper();
	    TriangleHHDegreeReducer reducer = new TriangleHHDegreeReducer();
	    
	    mapDriver = MapDriver.newMapDriver(mapper);
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapreduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
	    
	  }
	 
	  @Test
	  public void testMapper() throws IOException {
		    mapDriver.withInput(new LongWritable(1), new Text("9465097	12566713"));
		    mapDriver.withOutput(new LongWritable(9465097),new LongWritablePair(9465097,12566713));
	    mapDriver.runTest();
	  }

	 
	  @Test
	  public void testReducer() throws IOException {
	    List<LongWritablePair> values = new ArrayList<LongWritablePair>();
	    values.add(new LongWritablePair(9465097,12566713));
	    values.add(new LongWritablePair(9465097,12566714));
	    values.add(new LongWritablePair(9465097,12566715));

	    
	    reduceDriver.withInput(new LongWritable(9465097), values);
	    reduceDriver.withOutput(new LongWritable(9465097),new Text(":"+"12566713 12566714"));
	    reduceDriver.withOutput(new LongWritable(9465097),new Text(":"+"12566713 12566715"));
	    reduceDriver.withOutput(new LongWritable(9465097),new Text(":"+"12566714 12566715"));
	    reduceDriver.runTest();
	  }
	  
	  @Test
	  public void testMapperReducer() throws IOException {
		  mapreduceDriver.withInput(new LongWritable(1), new Text("9465097	12566713"));
		  mapreduceDriver.withInput(new LongWritable(2), new Text("9465097	12566714"));
		  mapreduceDriver.withInput(new LongWritable(3), new Text("9465097	12566715"));
		  mapreduceDriver.withInput(new LongWritable(4), new Text("12566713	 12566715"));
		  mapreduceDriver.withInput(new LongWritable(4), new Text("12566713	 12566716"));
		  mapreduceDriver.withOutput(new LongWritable(9465097),new Text(":"+"12566713 12566714"));
		  mapreduceDriver.withOutput(new LongWritable(9465097),new Text(":"+"12566713 12566715"));
		  mapreduceDriver.withOutput(new LongWritable(9465097),new Text(":"+"12566714 12566715"));
		  mapreduceDriver.withOutput(new LongWritable(12566713),new Text(":"+"12566715 12566716"));

		  mapreduceDriver.runTest();
	  }

	  
}
