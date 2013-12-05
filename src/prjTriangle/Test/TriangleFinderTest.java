package prjTriangle.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import prjTriangle.LongWritablePair;
import prjTriangle.TriangleFinderMapper;
import prjTriangle.TriangleFinderReducer;


public class TriangleFinderTest extends TestCase {

	 	MapDriver<LongWritable, Text, LongWritable, LongWritablePair > mapDriver;
	 	ReduceDriver<LongWritable, LongWritablePair, Text,LongWritable> reduceDriver;
	 	MapReduceDriver<LongWritable, Text, LongWritable, LongWritablePair, Text, LongWritable> mapreduceDriver;
	 
	  @Before
	  public void setUp() {
	    TriangleFinderMapper mapper = new TriangleFinderMapper();
	    TriangleFinderReducer reducer = new TriangleFinderReducer();
	    
	    mapDriver = MapDriver.newMapDriver(mapper);
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapreduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
	    
	  }
	 
	  @Test
	  public void testMapper() throws IOException {
		    mapDriver.withInput(new LongWritable(1), new Text("9465097	12566713"));
		    mapDriver.withOutput(new LongWritable(9465097),new LongWritablePair(9465097,12566713));
		    mapDriver.withOutput(new LongWritable(12566713),new LongWritablePair(9465097,12566713));
	    mapDriver.runTest(false);
	  }

	 
	  @Test
	  public void testReducer_3DistinctInputPair_Return3Pairs() throws IOException {
	    List<LongWritablePair> values = new ArrayList<LongWritablePair>();
	    values.add(new LongWritablePair(9465097,12566713));
	    values.add(new LongWritablePair(9465097,12566715));
	    values.add(new LongWritablePair(9465097,12566716));
	    reduceDriver.withInput(new LongWritable(9465097), values);
	    reduceDriver.withOutput(new Text("12566713 12566715"),new LongWritable(9465097));
	    reduceDriver.withOutput(new Text("12566713 12566716"),new LongWritable(9465097));
	    reduceDriver.withOutput(new Text("12566715 12566716"),new LongWritable(9465097));
	    reduceDriver.runTest();
	  }
		 
	  @Test
	  public void testReducer_5DistinctInputPair_Return10Pairs() throws IOException {
	    List<LongWritablePair> values = new ArrayList<LongWritablePair>();
	    values.add(new LongWritablePair(9465097,12566713));
	    values.add(new LongWritablePair(9465097,12566714));
	    values.add(new LongWritablePair(9465097,12566715));
	    values.add(new LongWritablePair(9465097,12566716));
	    values.add(new LongWritablePair(9465097,12566717));
	    reduceDriver.withInput(new LongWritable(9465097), values);
	    reduceDriver.withOutput(new Text("12566713 12566714"),new LongWritable(9465097));
	    reduceDriver.withOutput(new Text("12566713 12566715"),new LongWritable(9465097));
	    reduceDriver.withOutput(new Text("12566713 12566716"),new LongWritable(9465097));
	    reduceDriver.withOutput(new Text("12566713 12566717"),new LongWritable(9465097));
	    reduceDriver.withOutput(new Text("12566714 12566715"),new LongWritable(9465097));
	    reduceDriver.withOutput(new Text("12566714 12566716"),new LongWritable(9465097));
	    reduceDriver.withOutput(new Text("12566714 12566717"),new LongWritable(9465097));
	    reduceDriver.withOutput(new Text("12566715 12566716"),new LongWritable(9465097));
	    reduceDriver.withOutput(new Text("12566715 12566717"),new LongWritable(9465097));
	    reduceDriver.withOutput(new Text("12566716 12566717"),new LongWritable(9465097));
	    reduceDriver.runTest(false);
	  }
	  
	  @Test
	  public void testReducer_1InputPair_ReturnEmptyOut() throws IOException {
	    List<LongWritablePair> values = new ArrayList<LongWritablePair>();
	    values.add(new LongWritablePair(9465097,12566713));
	    reduceDriver.withInput(new LongWritable(9465097), values);	
	    final List<Pair< Text,LongWritable>> result = reduceDriver.run();
	    assertEquals(0, result.size());
	  }
	  @Test
	  public void testReducerEmptyOut() throws IOException {
	    List<LongWritablePair> values = new ArrayList<LongWritablePair>();
	    values.add(new LongWritablePair(9465097,12566713));
	    reduceDriver.withInput(new LongWritable(9465097), values);	
	    final List<Pair< Text,LongWritable>> result = reduceDriver.run();
	    assertEquals(0, result.size());
	  }
	  @Test
	  public void testMapperReducer() throws IOException {
		  mapreduceDriver.withInput(new LongWritable(1), new Text("1 2"));
		  mapreduceDriver.withInput(new LongWritable(2), new Text("2 3"));
		  mapreduceDriver.withInput(new LongWritable(4), new Text("3 4"));
		  mapreduceDriver.withOutput(new Text("1 3"), new LongWritable(2));
		  mapreduceDriver.withOutput(new Text("2 4"), new LongWritable(3));

		  mapreduceDriver.runTest();
	  }
}
