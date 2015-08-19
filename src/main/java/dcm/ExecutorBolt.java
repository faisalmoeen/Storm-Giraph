package dcm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import bsp.BSPState;
import bsp.BoltRequestProcessor;
import graph.model.Computation;
import graph.model.Edge;
import graph.model.Vertex;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by faisal on 8/7/15.
 */
public class ExecutorBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private int m;
    private double e;
    private HashMap<Long,Vertex> vertexHashMap;
    private Computation computation;
    private BoltRequestProcessor boltRequestProcessor;
    private BSPState bspState;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        m = ((Double)(conf.get("m"))).intValue();
        e = (Double)(conf.get("e"));
        vertexHashMap = new HashMap<Long, Vertex>();
        computation = new Computation();
        BoltRequestProcessor boltRequestProcessor = new BoltRequestProcessor();
        computation.init(conf, boltRequestProcessor);
        bspState = new BSPState();
    }

    public void execute(Tuple tuple) {
        if(tuple.getSourceComponent().compareTo("source")==0) {
            System.out.println("got the edge from source");

            Edge edge = (Edge) tuple.getValueByField("edge");
            System.out.println(edge.getSource() + "," + edge.getTarget() + "," + edge.getProperty());
            if (!vertexHashMap.containsKey(edge.getSource())) {
                Vertex v = new Vertex(edge.getSource());
                computation.addEdge(v,edge.getTarget(),edge.getProperty());
            } else {
                Vertex v = vertexHashMap.get(edge.getSource());
                computation.addEdge(v,edge.getTarget(),edge.getProperty());
            }
        }
        else if(tuple.getSourceComponent().compareTo("executor")==0){

        }
        if(boltRequestProcessor.hasMessages(bspState.getSuperStep())){

        }
        Values v = new Values();
        v.add(1L);
        v.add(null);
        System.out.println();
        _collector.emit(tuple, v);
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("BSPMessages",new Fields("superStep","vertexId","value"));
        declarer.declareStream("events",new Fields("time","clusters"));
    }



}