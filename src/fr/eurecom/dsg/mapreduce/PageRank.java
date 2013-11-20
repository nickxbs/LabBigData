package fr.eurecom.dsg.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
 
public class PageRank extends Configured implements Tool {
 
    public static String OUT = "outfile";
    public static String IN = "inputlarger";
 
    public static class TheMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
 
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //From slide 20 of Graph Algorithms with MapReduce (by Jimmy Lin, Univ @ Maryland)
            //Key is node n
            //Value is D, Points-To
            //For every point (or key), look at everything it points to.
            //Emit or write to the points to variable with the current distance + 1
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
 
            word.set("NODES "+nodes);//tells me to append on the final tally
            context.write( new LongWritable( Integer.parseInt( sp[0] ) ), word );
            word.clear();
 
        }
    }
 
    public static class TheReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //From slide 20 of Graph Algorithms with MapReduce (by Jimmy Lin, Univ @ Maryland)
            //The key is the current point
            //The values are all the possible distances to this point
            //we simply emit the point and the minimum distance value
 
            String nodes = "";
            Text word = new Text();
            Double s= (double) 0;
 
            for (Text val : values) {//looks like NODES/VALUES 1 0 2:3:, we need to use the first as a key
                String[] sp = val.toString().split(" ");//splits on space
                //look at first value
                if(sp[0].equalsIgnoreCase("NODES")){
                    nodes = null;
                    nodes = sp[1];
                }else if(sp[0].equalsIgnoreCase("VALUE")){
                    s =s+Double.parseDouble(sp[1]) ;
                }
            }
            word.set(s+" "+nodes);
            context.write(key, word);
            word.clear();
        }
    }
 
    //Almost exactly from http://hadoop.apache.org/mapreduce/docs/current/mapred_tutorial.html
    public int run(String[] args) throws Exception {
        //http://code.google.com/p/joycrawler/source/browse/NetflixChallenge/src/org/niubility/learning/knn/KNNDriver.java?r=242
        getConf().set("mapred.textoutputformat.separator", " ");//make the key -> value space separated (for iterations)
 
        //set in and out to args.
        if(args.length==0)
        {
            IN = "/home/student/INPUT/graph/N-sample-small.txt";
            OUT = "/home/student/OUTPUT/wordcount";
        }
        else{
            IN = args[0];
            OUT = args[1];
        }
 
        String infile = IN;
        String outputfile = OUT + System.nanoTime();
 
        boolean isdone = false;
        boolean success = false;
 
        HashMap <Integer, Double> _map = new HashMap<Integer, Double>();
 
        while(isdone == false){
 
            Job job = new Job(getConf());
            job.setJarByClass(PageRank.class);
            job.setJobName("PageRank");
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(TheMapper.class);
            job.setReducerClass(TheReducer.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
 
            FileInputFormat.addInputPath(job, new Path(infile));
            FileOutputFormat.setOutputPath(job, new Path(outputfile));
 
            success = job.waitForCompletion(true);
 
            //remove the input file
            //http://eclipse.sys-con.com/node/1287801/mobile
            if(infile != IN){
                String indir = infile.replace("part-r-00000", "");
                Path ddir = new Path(indir);
                FileSystem dfs = FileSystem.get(getConf());
                dfs.delete(ddir, true);
            }
 
            infile = outputfile+"/part-r-00000";
            outputfile = OUT + System.nanoTime();
 
            //do we need to re-run the job with the new input file??
            //http://www.hadoop-blog.com/2010/11/how-to-read-file-from-hdfs-in-hadoop.html
            isdone = true;//set the job to NOT run again!
            Path ofile = new Path(infile);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(ofile)));
 
            HashMap<Integer, Double> imap = new HashMap<Integer, Double>();
            String line=br.readLine();
            while (line != null){
                //each line looks like 0 1 2:3:
                //we need to verify node -> distance doesn't change
                String[] sp = line.split(" ");
                int node = Integer.parseInt(sp[0]);
                Double rank = Double.parseDouble(sp[1]);
                imap.put(node, rank);
                line=br.readLine();
            }
            if(_map.isEmpty()){
                //first iteration... must do a second iteration regardless!
                isdone = false;
            }else{
                //http://www.java-examples.com/iterate-through-values-java-hashmap-example
                //http://www.javabeat.net/articles/33-generics-in-java-50-1.html
                Iterator<Integer> itr = imap.keySet().iterator();
                while(itr.hasNext()){
                    int key = itr.next();
                    Double val = imap.get(key);
                    if(Math.abs(_map.get(key)- val)>0.001){
                        //values aren't the same... we aren't at convergence yet
                        isdone = false;
                    }
                }
            }
            if(isdone == false){
                _map.putAll(imap);//copy imap to _map for the next iteration (if required)
            }
        }
 
        return success ? 0 : 1;
    }
 
  
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PageRank(), args));
    }
}