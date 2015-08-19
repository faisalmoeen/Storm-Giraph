package bsp;

/**
 * Created by faisalorakzai on 16/08/15.
 */
public class BSPState {
    private Long superStep;

    public BSPState() {
        superStep=0L;
    }

    public Long getSuperStep() {
        return superStep;
    }

    public void setSuperStep(Long superStep) {
        this.superStep = superStep;
    }
}
