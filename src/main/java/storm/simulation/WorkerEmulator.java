package storm.simulation;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.log4j.Logger;
import storm.giraph.ContextConstructor;
import storm.giraph.GraphTaskManagerCallable;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by faisal on 1/4/16.
 */
public class WorkerEmulator<I extends WritableComparable, V extends Writable,
        E extends Writable> {
    OutputCollector _collector;
    TopologyContext _context;
    Map _conf;
    Mapper.Context mapContext = null;
    Integer taskId;
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(WorkerEmulator.class);
    /** Manage the framework-agnostic Giraph tasks for this job run */
    private GraphTaskManager<I, V, E> graphTaskManager;
//    MapContext<Object, Object, Object, Object> mapContext = null;

    public void prepare(Map conf, int taskId, int taskType) {
        _conf = conf;

        this.taskId = taskId;
        int attempt = 0;
        int jobId = 1;
        TaskAttemptID taskAttemptID = new TaskAttemptID("jobtracker",jobId,true,taskId,attempt);
        try {
            mapContext = (Mapper.Context) ContextConstructor.getMapContext(conf, taskAttemptID);
        } catch (Exception e) {
            e.printStackTrace();
        }
        assert mapContext != null;
//        mapContext.getConfiguration().set("mapred.job.id","job-"+System.currentTimeMillis()+"-0001");
        mapContext.getConfiguration().set("mapred.task.partition", this.taskId.toString());
//        if((taskId != 0) && (mapContext.getConfiguration().get("storm.zookeeper.servers") == null)) {
//            mapContext.getConfiguration().set("storm.zookeeper.servers", "localhost");
//            mapContext.getConfiguration().set("storm.zookeeper.port", "22181");
//        }
        if((taskId != 0) && (mapContext.getConfiguration().get("giraph.zkList") == null)) {
            mapContext.getConfiguration().set("giraph.zkList", "localhost:22181");
//            mapContext.getConfiguration().set("storm.zookeeper.port", "22181");
        }

        graphTaskManager = new GraphTaskManager<I, V, E>(mapContext);

        // Setting the default handler for uncaught exceptions.
        Thread.setDefaultUncaughtExceptionHandler(
                graphTaskManager.createUncaughtExceptionHandler());


        try {
            graphTaskManager.setup(null,taskType);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        LOG.info("*****************taskIndex="+taskId+"********************");
        runGtmThread();

    }

    public void cleanup() {

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
        GraphTaskManagerCallable gtmc = new GraphTaskManagerCallable(graphTaskManager);
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        Future<Integer> result = executor.submit(gtmc);
    }
}
