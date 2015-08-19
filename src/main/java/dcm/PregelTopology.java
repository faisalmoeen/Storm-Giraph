/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dcm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class PregelTopology {

    public static String inputFilePath="/tmp/edges.txt";
    public static double m = 3;
    public static long k = 180;
    public static double e = 0.0006;
    public static int s = 1; //sampling rate

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source", new EdgeSource(), 1);
        builder.setBolt("executor", new ExecutorBolt(), 1)
                .shuffleGrouping("source")
                .shuffleGrouping("executor")
                .shuffleGrouping("master");
        builder.setBolt("master", new MasterBolt(), 1)
                .shuffleGrouping("executor");

        Config conf = new Config();
//        conf.setDebug(true);
//        conf.put(Config.TOPOLOGY_DEBUG, true);
        conf.put("inputFilePath",inputFilePath);
        conf.put("m",m);
        conf.put("k",k);
        conf.put("e",e);
        conf.put("s",s);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(4);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {

            System.out.println("Initiate Cluster");
            LocalCluster cluster = new LocalCluster();
            System.out.println("Cluster Initiated");
            System.out.println("Start Time = " + System.currentTimeMillis());
            conf.put("startTime",System.currentTimeMillis());
            cluster.submitTopology("test", conf, builder.createTopology());
            System.out.println("Topology Submitted");
            Utils.sleep(100000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
