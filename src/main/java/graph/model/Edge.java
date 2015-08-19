package graph.model;

/**
 * Created by faisal on 8/14/15.
 */
public class Edge {
    private Long source;
    private Long target;
    private Long property;

    public Edge(long source, long target, Long property) {
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

    public Long getProperty() {
        return property;
    }

    public void setProperty(Long property) {
        this.property = property;
    }
}
