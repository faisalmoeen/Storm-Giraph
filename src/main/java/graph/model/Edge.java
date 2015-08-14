package graph.model;

/**
 * Created by faisal on 8/14/15.
 */
public class Edge {
    private Long source;
    private Long target;
    private Object property;

    public Edge(long source, long target, Object property) {
        this.source = source;
        this.target = target;
        this.property = property;
    }

    public Long getSource() {
        return source;
    }

    public void setSource(Long source) {
        this.source = source;
    }

    public Long getTarget() {
        return target;
    }

    public void setTarget(Long target) {
        this.target = target;
    }

    public Object getProperty() {
        return property;
    }

    public void setProperty(Object property) {
        this.property = property;
    }
}
