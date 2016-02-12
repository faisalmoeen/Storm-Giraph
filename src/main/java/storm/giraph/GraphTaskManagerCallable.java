package storm.giraph;

import org.apache.giraph.graph.GraphTaskManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Created by faisal on 1/16/16.
 */
public class GraphTaskManagerCallable implements Callable<Integer> {
    private GraphTaskManager graphTaskManager;
    private static final Logger LOG = Logger.getLogger(GraphTaskManagerCallable.class);
    public GraphTaskManagerCallable(GraphTaskManager gtm) {
        this.graphTaskManager = gtm;
    }

    @Override
    public Integer call() throws Exception {
        try {
            graphTaskManager.execute();
            graphTaskManager.cleanup();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (RuntimeException e){
            LOG.error("Caught an unrecoverable exception " + e.getMessage(), e);
            LOG.info("Task partition id:"+graphTaskManager.getConf().getTaskPartition());
            graphTaskManager.zooKeeperCleanup();
            graphTaskManager.workerFailureCleanup();
            throw new IllegalStateException(
                    "run: Caught an unrecoverable exception " + e.getMessage(), e);
        }
        return null;
    }
}
