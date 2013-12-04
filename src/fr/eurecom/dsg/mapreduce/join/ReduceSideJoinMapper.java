package fr.eurecom.dsg.mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


class ReduceSideJoinMapper extends
Mapper<LongWritable, Text, TextPair, IntWritable> {

@Override
protected void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {

String line = value.toString();
line = line.replaceAll("^\\s+", "");
String[] sp = line.split("\\s+");// splits on TAB

context.write(new TextPair(sp[1], "0"),
		new IntWritable(Integer.parseInt(sp[0])));// followed
context.write(new TextPair(sp[0], "1"),
		new IntWritable(Integer.parseInt(sp[1])));// follower
}
}
