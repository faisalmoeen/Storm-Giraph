package storm;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.giraph.ContextConstructor;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by faisal on 1/4/16.
 */
public class SourceSpout extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(SourceSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;
    String inputPath;
    WeightedEdgeReader edgeReader;
    BufferedReader br;
    String line;
    TopologyContext _context;
    Map _conf;
    Mapper.Context mapContext = null;
    Integer taskId;
    /** Class logger */

    public SourceSpout() {
        this(true);
    }

    public SourceSpout(boolean isDistributed) {
        _isDistributed = isDistributed;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
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
        inputPath = (String)conf.get("inputPath");
        JsonLongDoubleFloatDoubleVertexInputFormat inputFormat = new JsonLongDoubleFloatDoubleVertexInputFormat();
//        inputFormat.se
//        edgeReader = new WeightedEdgeReader(inputPath);
        try {
            br = new BufferedReader(new FileReader(new File(inputPath)));

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
    }

    public void nextTuple() {
        try {
            if((line = br.readLine()) != null) {
                _collector.emit("s2p", new Values(line));
            }
            else{

                Utils.sleep(999999999999l);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        _collector.emit("s2p", new Values("1"));
        _collector.emit("s2p", new Values("2"));
        _collector.emit("s2p", new Values("3"));
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("s2p",new Fields("partitionId"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }
}
