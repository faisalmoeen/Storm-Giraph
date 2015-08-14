package dcm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;

/**
 * Created by faisal on 8/7/15.
 */
public class ClusteringBolt extends BaseRichBolt {
    OutputCollector _collector;
    int m;
    double e;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        m = ((Double)(conf.get("m"))).intValue();
        e = (Double)(conf.get("e"));
    }

    public void execute(Tuple tuple) {
        Values v = new Values();
        v.add(1);
        v.add(null);
        _collector.emit(tuple, v);
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time","clusters"));
    }



}