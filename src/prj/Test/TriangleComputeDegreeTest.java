package prj.Test;

import org.junit.Before;
import org.junit.Test;
import prj.TriangleComputeDegreeMapper;
import prj.TriangleComputeDegreeReducer;
import junit.framework.TestCase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
 
public class TriangleComputeDegreeTest extends TestCase {

	 	MapDriver<LongWritable, Text, LongWritable, IntWritable> mapDriver;
	 	ReduceDriver<LongWritable, IntWritable, LongWritable, IntWritable> reduceDriver;
	 	MapReduceDriver<LongWritable, Text, LongWritable, IntWritable, LongWritable, IntWritable> mapreduceDriver;
	 
	  @Before
	  public void setUp() {
	    TriangleComputeDegreeMapper mapper = new TriangleComputeDegreeMapper();
	    TriangleComputeDegreeReducer reducer = new TriangleComputeDegreeReducer();
	    
	    mapDriver = MapDriver.newMapDriver(mapper);
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapreduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
	    
	  }
	 
	  @Test
	  public void testMapper() throws IOException {
		    mapDriver.withInput(new LongWritable(1), new Text("9465097	12566713"));
		    mapDriver.withOutput(new LongWritable(9465097),new IntWritable(1));
		    mapDriver.withOutput(new LongWritable(12566713),new IntWritable(1));
	    mapDriver.runTest();
	  }

	 
	  @Test
	  public void testReducer() throws IOException {
	    List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(1));
	    
	    reduceDriver.withInput(new LongWritable(1), values);
	    reduceDriver.withOutput(new LongWritable(1),new IntWritable(3));
	    reduceDriver.runTest();
	  }
	  
	  @Test
	  public void testMapperReducer() throws IOException {
		  mapreduceDriver.withInput(new LongWritable(1), new Text("9465097	12566713"));
		  mapreduceDriver.withInput(new LongWritable(2), new Text("9465097	12566714"));
		  mapreduceDriver.withInput(new LongWritable(3), new Text("9465097	12566715"));
		  mapreduceDriver.withOutput(new LongWritable(9465097),new IntWritable(3));
		  mapreduceDriver.withOutput(new LongWritable(12566713),new IntWritable(1));
		  mapreduceDriver.withOutput(new LongWritable(12566714),new IntWritable(1));
		  mapreduceDriver.withOutput(new LongWritable(12566715),new IntWritable(1));
		  mapreduceDriver.runTest();
	  }

	  
}
