package graph.model;

import bsp.BoltRequestProcessor;

import java.util.Map;

/**
 * Created by faisalorakzai on 15/08/15.
 */
public class Computation {
    private Map conf;
    private BoltRequestProcessor boltRequestProcessor;

    public void init(Map conf, BoltRequestProcessor boltRequestProcessor){
        this.conf = conf;
        this.boltRequestProcessor = boltRequestProcessor;
    }

    public void compute(){

    }

    public void sendMessage(Long id, Long value){
        boltRequestProcessor.sendMessage(id,value);
    }

    public void addEdge(Vertex v1, Long targetId, Long value){
        v1.addEdge(targetId,value);
    }
}
