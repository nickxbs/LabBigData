package fr.eurecom.dsg.mapreduce.Tests;

import org.junit.Before;
import org.junit.Test;
import fr.eurecom.dsg.mapreduce.PageRankMapper;
import fr.eurecom.dsg.mapreduce.PageRankReducer;
import junit.framework.TestCase;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
 
public class PageRankTest extends TestCase {

	 	MapDriver<LongWritable, Text, LongWritable, Text> mapDriver;
	 	ReduceDriver<LongWritable, Text, LongWritable, Text> reduceDriver;
	 	MapReduceDriver<LongWritable, Text, LongWritable, Text, LongWritable, Text> mapreduceDriver;
	 
	  @Before
	  public void setUp() {
	    PageRankMapper mapper = new PageRankMapper();
	    PageRankReducer reducer = new PageRankReducer();
	    
	    mapDriver = MapDriver.newMapDriver(mapper);;
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapreduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
	    
	  }
	 
	  @Test
	  public void testMapper() throws IOException {
	    mapDriver.withInput(new LongWritable(), new Text("9465097	0.2	12566713 11158207"));
	    mapDriver.withOutput(new LongWritable(12566713),new Text("VALUE 0.1"));
	    mapDriver.withOutput(new LongWritable(11158207),new Text("VALUE 0.1"));
	    mapDriver.withOutput(new LongWritable(9465097),new Text("VALUE 0.2"));
	    mapDriver.withOutput(new LongWritable(9465097),new Text("NODES 12566713 11158207"));
	    mapDriver.runTest();
	  }
	  @Test
	  public void testMapperCount() throws IOException {
	    mapDriver.withInput(new LongWritable(), new Text("9465097	0.2	12566713 11158207"));
	    final List<Pair<LongWritable, Text>> result = mapDriver.run();
	    assertEquals(4, result.size());
	  }

	  
	 /*
	  @Test
	  public void testReducer() throws IOException {
	    List<Text> values = new ArrayList<Text>();
	    values.add(new Text("1"));
	    values.add(new Text("1"));
	    reduceDriver.withInput(new LongWritable(), values);
	    reduceDriver.withOutput(new LongWritable(),new Text());
	    reduceDriver.runTest();
	  }
	  */
}
