package Test;

import java.io.IOException;


import junit.framework.TestCase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.Before;
import org.junit.Test;


import prjTriangle.LongWritableTriplet;
import prjTriangle.TriangleFinderMapper2;

import prjTriangle.TriangleFinderReducer2;


public class TriangleFinderTest2 extends TestCase {

	 	MapDriver<LongWritable, Text, LongWritableTriplet, LongWritable> mapDriver;
	 	ReduceDriver<LongWritableTriplet, LongWritable, Text,Text> reduceDriver;
	 	MapReduceDriver<LongWritable, Text, LongWritableTriplet, LongWritable, Text, Text> mapreduceDriver;
	 	
	 
	  @Before
	  public void setUp() {
	    TriangleFinderMapper2 mapper = new TriangleFinderMapper2();
	    TriangleFinderReducer2 reducer = new TriangleFinderReducer2();
	    //TriangleFinderPartitioner partitioner = new TriangleFinderPartitioner();
	    
	   
	    mapDriver = MapDriver.newMapDriver(mapper);
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapreduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
	    
	  }
	 
	  @Test
	  public void testMapper2_PossibileTriangolo() throws IOException {
		    mapDriver.withInput(new LongWritable(1), new Text("1 3	2"));
		    mapDriver.withOutput(new LongWritableTriplet(new Long(1),new Long(3),new Long(1)),new LongWritable(2));
	    mapDriver.runTest(false);
	  }
	  @Test
	  public void testMapper2_Coppia() throws IOException {
		    mapDriver.withInput(new LongWritable(1), new Text("1 2"));
		    mapDriver.withOutput(new LongWritableTriplet(new Long(1),new Long(2),new Long(0)),new LongWritable(0));
	    mapDriver.runTest(false);
	  }
/*	 
	  @Test
	  public void testReducer_3DistinctInputPair_Return3Pairs() throws IOException {
	    List<LongWritableTriplet> values = new ArrayList<LongWritableTriplet>();
	    values.add(new LongWritableTriplet(1,3,0),2);
	    values.add(new LongWritableTriplet(1,3,-1),-1);
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
	  */
	  @Test
	  public void testMapperReducer() throws IOException {
		  mapreduceDriver.withInput(new LongWritable(1), new Text("1 3 2"));
		  mapreduceDriver.withInput(new LongWritable(2), new Text("1 3"));
		  mapreduceDriver.withOutput(new Text("1 2 3"), new Text());

		  mapreduceDriver.runTest();
	  }
	  
}
