package prjTriangleAdv;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by nicola on 15/06/17.
 */
public class Runner {
    private Path _outOther2;
    private Path _outOther;
    private Path _outHH;
    private Configuration _conf;
    private Path _outCount;
    private Path _outDegree;
    private Path _outputDir;
    private Path _inputPath;
    private Path _partialDir;
    private int _b;
    private Finder _finder;
    private FileSystem _dfs;
    public Runner(Path outputDir, Path inputPath, Path partialDir, int b,Finder finder) throws IOException {
        _outputDir = outputDir;
        _inputPath = inputPath;
        _partialDir = partialDir;
        _b = b;
        _finder = finder;
        _conf = finder.getConf();
        _dfs = FileSystem.get(_conf);

        _outDegree = new Path(_partialDir.toString() + "/degree");
        _outCount = new Path(_partialDir.toString() + "/countedges");
        _outHH= new Path(_outputDir.toString() + "_out-hh");
        _outOther= new Path(_outputDir.toString() + "_out-ot");
        _outOther2= new Path(_outputDir.toString() + "_out-ot2");

    }
    public Runner runCounter() throws IOException, ClassNotFoundException, InterruptedException {
        Job jobCountNodes = new Job(_conf, "jobCountEdges");
        jobCountNodes.setJarByClass(Finder.class);

        jobCountNodes.setInputFormatClass(TextInputFormat.class);

        jobCountNodes.setMapperClass(MapperCount.class);
        jobCountNodes.setMapOutputKeyClass(IntWritable.class);
        jobCountNodes.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobCountNodes, _inputPath);

        jobCountNodes.setReducerClass(ReducerCount.class);
        jobCountNodes.setOutputKeyClass(Text.class);
        jobCountNodes.setOutputValueClass(Text.class);

        jobCountNodes.setNumReduceTasks(_b);
        jobCountNodes.setOutputFormatClass(TextOutputFormat.class);


        Path tmpoutCountPath = new Path(_partialDir.toString() + "_partial-countnodes");
        if (_dfs.exists(tmpoutCountPath))
            _dfs.delete(tmpoutCountPath, true);
        FileOutputFormat.setOutputPath(jobCountNodes, tmpoutCountPath);
        jobCountNodes.waitForCompletion(true);
        if (_dfs.exists(_outCount))
            _dfs.delete(_outCount, true);
        FileUtil.copyMerge(_dfs,tmpoutCountPath,_dfs, _outCount, false,_conf,"");

        return this;
    }
    public Runner runDegree()throws IOException, ClassNotFoundException, InterruptedException {
        Job jobDegree = new Job(_conf, "RunDegree");
        jobDegree.setJarByClass(Finder.class);

        jobDegree.setInputFormatClass(TextInputFormat.class);

        jobDegree.setMapperClass(MapperDegree.class);
        jobDegree.setMapOutputKeyClass(IntWritable.class);
        jobDegree.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobDegree, _inputPath);

        jobDegree.setGroupingComparatorClass(GroupingComparatorDegree.class);
        jobDegree.setPartitionerClass(PartitionerDegree.class);

        jobDegree.setReducerClass(ReducerDegree.class);
        jobDegree.setOutputKeyClass(Text.class);
        jobDegree.setOutputValueClass(Text.class);

        jobDegree.setNumReduceTasks(_b);
        jobDegree.setOutputFormatClass(TextOutputFormat.class);
        Path tmpoutDegree = new Path(_partialDir.toString() + "_partial-degree");
        if (_dfs.exists(tmpoutDegree))
            _dfs.delete(tmpoutDegree, true);

        FileOutputFormat.setOutputPath(jobDegree, tmpoutDegree);
        jobDegree.waitForCompletion(true);
        if (_dfs.exists(_outDegree))
            _dfs.delete(_outDegree, true);
        FileUtil.copyMerge(_dfs,tmpoutDegree,_dfs,_outDegree, false,_conf,"");
        return this;
    }
    public Runner runHH() throws IOException, ClassNotFoundException, InterruptedException {

        Job jobHH = new Job(_conf, "jobHH");
        jobHH.setJarByClass(Finder.class);

        jobHH.setInputFormatClass(TextInputFormat.class);

        jobHH.setMapperClass(MapperHeavyHitter.class);
        jobHH.setMapOutputKeyClass(BucketItemDegree.class);
        jobHH.setMapOutputValueClass(BucketItemDegree.class);

        jobHH.setPartitionerClass(PartitionerBucketDegree.class);
        jobHH.setSortComparatorClass(ComparatorBucketItemDegree.class);

        jobHH.setReducerClass(ReducerHeavyHitter.class);
        jobHH.setOutputKeyClass(Text.class);
        jobHH.setOutputValueClass(Text.class);

        jobHH.setNumReduceTasks(((int) Math.pow(_b, 3)));
        jobHH.setOutputFormatClass(TextOutputFormat.class);

        DistributedCache.addCacheFile(_outCount.toUri(), jobHH.getConfiguration());
        DistributedCache.addCacheFile(_outDegree.toUri(), jobHH.getConfiguration());
        FileInputFormat.addInputPath(jobHH, _inputPath);
        FileInputFormat.addInputPath(jobHH, _inputPath);

        if (_dfs.exists(_outHH))
            _dfs.delete(_outHH, true);
        FileOutputFormat.setOutputPath(jobHH, _outHH);
        jobHH.waitForCompletion(true);

        return this;
    }
    public Runner runOthers1(Class map,Class reduce, Class key, Class partitioner, Class sortcomparator) throws IOException, ClassNotFoundException, InterruptedException {

        Job jobOthers1 = new Job(_conf, "jobOthers");
        jobOthers1.setJarByClass(Finder.class);

        jobOthers1.setInputFormatClass(TextInputFormat.class);

        jobOthers1.setMapperClass(map);
        jobOthers1.setMapOutputKeyClass(key);
        jobOthers1.setMapOutputValueClass(key);

        //jobOthers.setGroupingComparatorClass(GroupingComparatorOthers2.class);
        jobOthers1.setPartitionerClass(partitioner);
        jobOthers1.setSortComparatorClass(sortcomparator);

        jobOthers1.setReducerClass(reduce);
        jobOthers1.setOutputKeyClass(Text.class);
        jobOthers1.setOutputValueClass(Text.class);

        jobOthers1.setNumReduceTasks(((int) Math.pow(_b, 3)));
        jobOthers1.setOutputFormatClass(TextOutputFormat.class);

        DistributedCache.addCacheFile(_outCount.toUri(), jobOthers1.getConfiguration());
        DistributedCache.addCacheFile(_outDegree.toUri(), jobOthers1.getConfiguration());
        FileInputFormat.addInputPath(jobOthers1, _inputPath);

        if (_dfs.exists(_outOther))
            _dfs.delete(_outOther, true);
        FileOutputFormat.setOutputPath(jobOthers1, _outOther);
        jobOthers1.waitForCompletion(true);

        return this;
    }
    public Runner runOthers2(Class map,Class reduce) throws IOException, ClassNotFoundException, InterruptedException {
        Job jobOthers2 = new Job(_conf, "jobOthers2");
        jobOthers2.setJarByClass(Finder.class);

        jobOthers2.setInputFormatClass(TextInputFormat.class);

        jobOthers2.setMapperClass(map);
        jobOthers2.setMapOutputKeyClass(KeyClosure.class);
        jobOthers2.setMapOutputValueClass(IntWritable.class);
        jobOthers2.setPartitionerClass(PartitionerKeyClosure.class );

        jobOthers2.setReducerClass(reduce);
        jobOthers2.setOutputKeyClass(Text.class);
        jobOthers2.setOutputValueClass(Text.class);
        jobOthers2.setGroupingComparatorClass(GroupingComparatorOthers2.class);
        jobOthers2.setSortComparatorClass(ComparatorKeyClosure.class);
        jobOthers2.setNumReduceTasks(((int) Math.pow(_b, 3)));
        jobOthers2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(jobOthers2, _outOther);

        if (_dfs.exists(_outOther2))
            _dfs.delete(_outOther2, true);
        FileOutputFormat.setOutputPath(jobOthers2, _outOther2);
        jobOthers2.waitForCompletion(true);
        return this;
    }
}
