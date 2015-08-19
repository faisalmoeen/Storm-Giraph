package graph.model;

import java.util.HashMap;
import java.util.List;

/**
 * Created by faisalorakzai on 15/08/15.
 */
public class Vertex {
    private HashMap<Long,Long> edges;
    private Long id;
    private Long value;

    public Vertex(Long id){
        this.id = id;
        this.edges = new HashMap<Long, Long>();
    }

    public Vertex(HashMap<Long, Long> edges, Long id) {
        this.edges = edges;
        this.id = id;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    public HashMap<Long, Long> getEdges() {
        return edges;
    }

    public void setEdges(HashMap<Long, Long> edges) {
        this.edges = edges;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void addEdge(Edge edge) {
        this.edges.put(edge.getTarget(),edge.getProperty());
    }

    public void addEdge(Long target, Long value) {
        this.edges.put(target,value);
    }
}
