package prj;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TriangleComputeDegreeMapper  extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
		 
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        	//9465097	12566713
            String line = value.toString();
    		line = line.replaceAll("^\\s+", "");
            String[] sp = line.split("\\s+");//splits on TAB

            context.write( new LongWritable( Integer.parseInt( sp[0] ) ), new IntWritable(1));
            context.write( new LongWritable( Integer.parseInt( sp[1] ) ), new IntWritable(1));
 
        }
    }