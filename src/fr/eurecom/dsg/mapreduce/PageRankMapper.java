package fr.eurecom.dsg.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper  extends Mapper<LongWritable, Text, LongWritable, Text> {
		 
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        	//9465097	0.010752688172043	12566713	11158207	11145916	11883199	12857908
            Text word = new Text();
            String line = value.toString();//looks like 1 0 2:3:
    		line = line.replaceAll("^\\s+", "");
            String[] sp = line.split("\\s+");//splits on TAB
            int nodesAdiacent=sp.length-2;
            String nodes="";
            double rank = Double.parseDouble(sp[1])/nodesAdiacent;
            		for(int j=2;j<sp.length;j++)
            		{
                        word.set("VALUE "+rank);//tells me to look at distance value
                        context.write(new LongWritable(Integer.parseInt(sp[j])), word);
                        word.clear();
                        nodes+=sp[j]+" ";
            		}
            //pass in current node's distance (if it is the lowest distance)
            word.set("VALUE "+sp[1]);
            context.write( new LongWritable( Integer.parseInt( sp[0] ) ), word );
            word.clear();
 
            word.set("NODES "+ nodes.trim());//tells me to append on the final tally
            context.write( new LongWritable( Integer.parseInt( sp[0] ) ), word );
            word.clear();
 
        }
    }