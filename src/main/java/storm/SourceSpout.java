package storm;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public SourceSpout() {
        this(true);
    }

    public SourceSpout(boolean isDistributed) {
        _isDistributed = isDistributed;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        inputPath = (String)conf.get("inputPath");
        edgeReader = new WeightedEdgeReader(inputPath);

    }

    public void close() {

    }

    public void nextTuple() {

        _collector.emit("s2p",new Values("1"));
//        _collector.emit("s2p",new Values("2"));
//        _collector.emit("s2p",new Values("3"));
        Utils.sleep(999999999999l);
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
