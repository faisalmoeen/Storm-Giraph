package dcm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.util.Map;

/**
 * Created by faisal on 8/7/15.
 */
public class MergingBolt extends BaseRichBolt{
    OutputCollector _collector;
    long tPrevious=-0;
    long s=1; //sampling rate

    int m;
    double e;
    long t;
    long k;
    long startTime;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        s = (Long)conf.get("s");
        m = ((Double)(conf.get("m"))).intValue();
        e = (Double)(conf.get("e"));
        k = (Long)(conf.get("k"));
        startTime = (Long)(conf.get("startTime"));
    }

    public void execute(Tuple tuple) {
//            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
        t = tuple.getLongByField("time").longValue();
//        System.out.println("received at merge: "+t);
        if(t!=tPrevious+s){
            return;
        }
        else{
            tPrevious=t;
        }
//        System.out.println(C);
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("convoy"));
    }
}
