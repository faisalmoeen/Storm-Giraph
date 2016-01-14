package storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Created by faisal on 1/4/16.
 */
public class ProcessorBolt <I extends WritableComparable, V extends Writable,
        E extends Writable> extends BaseRichBolt {
    OutputCollector _collector;
    TopologyContext _context;



    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _context = context;

    }

    public void execute(Tuple tuple) {
//        _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
        _collector.emit(tuple, new Values(tuple.getString(0) ));
        System.out.println("**************:Task ID:"+_context.getThisTaskId()+" Task Index:"+_context.getThisTaskIndex()+" received "+tuple.getString(0)+" from "+tuple.getSourceComponent()+" through streamid:"+tuple.getSourceStreamId());
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("p2p",new Fields("partitionId"));
    }


}
