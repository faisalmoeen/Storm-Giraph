package storm;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.giraph.WorkerBolt;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by faisal on 1/4/16.
 */
public class TopologyBuilderFactory {
    private static List<BoltDeclarer> boltList;
    public static TopologyBuilder createBuilder(int noOfSourceNodes, long noOfProcessorNodes){
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source", new SourceSpout(), noOfSourceNodes);
//        boltList = new ArrayList<>();
        builder.setBolt("worker", new WorkerBolt(), noOfProcessorNodes)
                .setNumTasks(noOfProcessorNodes)
                .fieldsGrouping("source","s2p",new Fields("partitionId"))
                .fieldsGrouping("worker","p2p",new Fields("partitionId"));
        return builder;
    }
}
