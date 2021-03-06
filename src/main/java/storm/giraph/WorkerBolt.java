package storm.giraph;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by faisal on 1/4/16.
 */
public class WorkerBolt<I extends WritableComparable, V extends Writable,
        E extends Writable> extends BaseRichBolt {
    OutputCollector _collector;
    TopologyContext _context;
    Map _conf;
    Mapper.Context mapContext = null;
    Integer taskId;
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(WorkerBolt.class);
    /** Manage the framework-agnostic Giraph tasks for this job run */
    private GraphTaskManager<I, V, E> graphTaskManager;
//    MapContext<Object, Object, Object, Object> mapContext = null;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _context = context;
        _conf = conf;

        taskId = _context.getThisTaskIndex();
        int attempt = 0;
        int jobId = 1;
        TaskAttemptID taskAttemptID = new TaskAttemptID("jobtracker",jobId,true,taskId,attempt);
        try {
            mapContext = (Mapper.Context) ContextConstructor.getMapContext(conf, taskAttemptID);
        } catch (Exception e) {
            e.printStackTrace();
        }
        assert mapContext != null;
        mapContext.getConfiguration().set("mapred.task.partition", taskId.toString());
        graphTaskManager = new GraphTaskManager<I, V, E>(mapContext);

        // Setting the default handler for uncaught exceptions.
        Thread.setDefaultUncaughtExceptionHandler(
                graphTaskManager.createUncaughtExceptionHandler());


        try {
            graphTaskManager.setup(null);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        LOG.info("*****************taskIndex="+taskId+"threadID:"+Thread.currentThread().getId()+"********************");
        runGtmThread();

    }

    @Override
    public void cleanup() {
        super.cleanup();

    }

    public void execute(Tuple tuple) {
        LOG.info("*****************taskIndex="+taskId+"********************");
//        runGtm();

//        _collector.emit(tuple, new Values(tuple.getString(0) ));
//        System.out.println("**************:Task ID:"+_context.getThisTaskId()+" Task Index:"+_context.getThisTaskIndex()+" received "+tuple.getString(0)+" from "+tuple.getSourceComponent()+" through streamid:"+tuple.getSourceStreamId());
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("p2p",new Fields("partitionId"));
    }

    public void runGtm(){
        try {
            graphTaskManager.execute();
            graphTaskManager.cleanup();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (RuntimeException e){
            LOG.error("Caught an unrecoverable exception " + e.getMessage(), e);
            graphTaskManager.zooKeeperCleanup();
            graphTaskManager.workerFailureCleanup();
            throw new IllegalStateException(
                    "run: Caught an unrecoverable exception " + e.getMessage(), e);
        }
    }

    public void runGtmThread(){
//        GraphTaskManagerCallable gtmc = new GraphTaskManagerCallable(graphTaskManager);
//        ExecutorService executor = Executors.newSingleThreadExecutor();
//        Future<Integer> result = executor.submit(gtmc);
        GraphTaskManagerCallable gtmc = new GraphTaskManagerCallable(graphTaskManager);
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        Future<Integer> result = executor.submit(gtmc);
    }
}
