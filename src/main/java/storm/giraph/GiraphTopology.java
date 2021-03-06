package storm.giraph;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.giraph.bsp.BspInputFormat;
import org.apache.giraph.bsp.BspOutputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.examples.SimpleShortestPathsComputation;
import org.apache.giraph.graph.GraphMapper;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import storm.TopologyBuilderFactory;

import java.io.File;
import java.io.IOException;

/**
 * Created by faisal on 1/4/16.
 */
public class GiraphTopology {
    public static Long numWorkers=3L;
    public static Configuration configuration=null;
    public static String jobID;
    public static Long numSources=1L;
    public static void main(String[] args) throws Exception {
        //Giraph job setup

//        //Storm topology setup
        TopologyBuilderFactory factory = new TopologyBuilderFactory();
        TopologyBuilder builder = factory.createBuilder(1, numWorkers);
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(numWorkers.intValue());
        conf.put("inputPath","/home/faisal/git/Storm-Giraph/src/main/resources/edges.txt");
        conf.put("mapred.map.tasks",numWorkers);
//        conf.put("storm.zookeeper.servers","localhost");
//        conf.put("storm.zookeeper.port","2181");
//        conf.put("storm.zookeeper.root", "/storm");
        conf.put("storm.zookeeper.session.timeout", 800000);
        conf.put("storm.zookeeper.connection.timeout", 600000);
//        conf.setMaxTaskParallelism(5);
        runGiraphJob(false, numSources, numWorkers);
//        conf.put("giraph.pure.yarn.job",true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
//            StormSubmitter.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(99999999999L);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

    public static Configuration runGiraphJob(boolean run, Long numSources, Long numWorkers) throws Exception{
        String inputPath = "/home/faisal/git/Storm-Giraph/src/main/resources/tiny_graph.txt";
        String outputPath = "/tmp/graph_out";
        jobID = System.currentTimeMillis()+"-0001";
        try {
            FileUtils.deleteDirectory(new File("/tmp/graph_out"));
        }catch (IOException e){
            e.printStackTrace();
        }
        GiraphConfiguration giraphConf = new GiraphConfiguration(new Configuration());

        giraphConf.setComputationClass(SimpleShortestPathsComputation.class);

        giraphConf.setVertexInputFormatClass(JsonLongDoubleFloatDoubleVertexInputFormat.class);
        try {
            GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path(inputPath));
        }catch (IOException e){
            e.printStackTrace();
        }
        giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
        giraphConf.setLocalTestMode(true);
        giraphConf.setWorkerConfiguration(1, 1, 100);
        giraphConf.setMaxNumberOfSupersteps(100);
        giraphConf.SPLIT_MASTER_WORKER.set(giraphConf, false);
        giraphConf.USE_OUT_OF_CORE_GRAPH.set(giraphConf, true);
        giraphConf.set("mapred.job.id","job-"+jobID);
        giraphConf.set("mapred.map.tasks",numWorkers.toString());
        giraphConf.set("giraph.maxWorkers",numWorkers.toString());
        giraphConf.set("giraph.maxSources",numSources.toString());
        giraphConf.set("giraph.minSources",numSources.toString());
//        giraphConf.setBoolean("giraph.pure.yarn.job",true);

//        giraphConf.set("storm.zookeeper.servers","localhost");
//        giraphConf.set("storm.zookeeper.port","22181");
//        giraphConf.set("zk.connectiontimeout.ms", "80000");
//        giraphConf.set("giraph.logLevel","debug");
//
//
// giraphConf.set("giraph.pure.yarn.job","true");
//        giraphConf.set("giraph.maxWorkers","0");
//        giraphConf.set("giraph.zkList","localhost:2181");

        String jobName = "GiraphDemo";
        GiraphJob giraphJob = new GiraphJob(giraphConf,jobName);
        FileOutputFormat.setOutputPath(giraphJob.getInternalJob(), new Path(outputPath));

//        MapContext mapContext = new MapContext(giraphConf,new TaskAttemptID("giraphJTID",1,true,12,13),);
        if(run){
            giraphJob.run(true);
            return null;
        }
        else {
            ImmutableClassesGiraphConfiguration conf = new ImmutableClassesGiraphConfiguration(giraphConf);
            Job submittedJob = new Job(conf, jobName);
            submittedJob.setNumReduceTasks(0);
            submittedJob.setMapperClass(GraphMapper.class);
            submittedJob.setInputFormatClass(BspInputFormat.class);
            submittedJob.setOutputFormatClass(BspOutputFormat.class);
            configuration  = submittedJob.getConfiguration();
            return configuration;
        }
    }

    public static Configuration getConfiguration(){
        if(configuration==null)
            try {
                configuration = runGiraphJob(false, numSources, numWorkers);
            } catch (Exception e) {
                e.printStackTrace();
            }
        return configuration;
    }
}
