package storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by faisal on 1/4/16.
 */
public class ActiveBSPTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilderFactory factory = new TopologyBuilderFactory();
        TopologyBuilder builder = factory.createBuilder(1, 3);
        Config conf = new Config();
        conf.setDebug(true);
        conf.put("inputPath","/home/faisal/git/Storm-Giraph/src/main/resources/edges.txt");

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
