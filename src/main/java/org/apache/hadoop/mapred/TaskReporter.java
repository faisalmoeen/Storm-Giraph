package org.apache.hadoop.mapred;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by faisal on 1/11/16.
 */
public class TaskReporter extends org.apache.hadoop.mapreduce.StatusReporter
        implements Runnable, Reporter {
    private TaskUmbilicalProtocol umbilical;
    private org.apache.hadoop.mapred.InputSplit split = null;
    private Progress taskProgress;
//    private JvmContext jvmContext;
    private Thread pingThread = null;
    private static final int PROGRESS_STATUS_LEN_LIMIT = 512;
    private boolean done = true;
    private Object lock = new Object();

    // Current counters
    private transient org.apache.hadoop.mapred.Counters counters = new Counters();

    /**
     * flag that indicates whether progress update needs to be sent to parent.
     * If true, it has been set. If false, it has been reset.
     * Using AtomicBoolean since we need an atomic read & reset method.
     */
    private AtomicBoolean progressFlag = new AtomicBoolean(false);


    public static final String MR_COMBINE_RECORDS_BEFORE_PROGRESS = "mapred.combine.recordsBeforeProgress";
    public static final long DEFAULT_MR_COMBINE_RECORDS_BEFORE_PROGRESS = 10000;

    @Override
    public void run() {

    }

    // Counters used by Task subclasses
    public static enum Counter {
        MAP_INPUT_RECORDS,
        MAP_OUTPUT_RECORDS,
        MAP_SKIPPED_RECORDS,
        MAP_INPUT_BYTES,
        MAP_OUTPUT_BYTES,
        MAP_OUTPUT_MATERIALIZED_BYTES,
        COMBINE_INPUT_RECORDS,
        COMBINE_OUTPUT_RECORDS,
        REDUCE_INPUT_GROUPS,
        REDUCE_SHUFFLE_BYTES,
        REDUCE_INPUT_RECORDS,
        REDUCE_OUTPUT_RECORDS,
        REDUCE_SKIPPED_GROUPS,
        REDUCE_SKIPPED_RECORDS,
        SPILLED_RECORDS,
        SPLIT_RAW_BYTES,
        CPU_MILLISECONDS,
        PHYSICAL_MEMORY_BYTES,
        VIRTUAL_MEMORY_BYTES,
        COMMITTED_HEAP_BYTES
    }

    //skip ranges based on failed ranges from previous attempts
    private org.apache.hadoop.mapred.SortedRanges skipRanges = new org.apache.hadoop.mapred.SortedRanges();
    private boolean skipping = false;
    private boolean writeSkipRecs = true;

    //currently processing record start index
    private volatile long currentRecStartIndex;
    private Iterator<Long> currentRecIndexIterator = skipRanges.skipRangeIterator();

//    TaskReporter(Progress taskProgress,
//                 TaskUmbilicalProtocol umbilical, JvmContext jvmContext) {
//        this.umbilical = umbilical;
//        this.taskProgress = taskProgress;
//        this.jvmContext = jvmContext;
//    }

    public TaskReporter(){

    }
    // getters and setters for flag
    void setProgressFlag() {
        progressFlag.set(true);
    }
    boolean resetProgressFlag() {
        return progressFlag.getAndSet(false);
    }
    public void setStatus(String status) {
        //Check to see if the status string
        // is too long and just concatenate it
        // to progress limit characters.
        if (status.length() > PROGRESS_STATUS_LEN_LIMIT) {
            status = status.substring(0, PROGRESS_STATUS_LEN_LIMIT);
        }
        taskProgress.setStatus(status);
        // indicate that progress update needs to be sent
        setProgressFlag();
    }
    public void setProgress(float progress) {
        taskProgress.set(progress);
        // indicate that progress update needs to be sent
        setProgressFlag();
    }

    public float getProgress() {
        return taskProgress.getProgress();
    };

    public void progress() {
        // indicate that progress update needs to be sent
        setProgressFlag();
    }
    public org.apache.hadoop.mapred.Counters.Counter getCounter(String group, String name) {
        org.apache.hadoop.mapred.Counters.Counter counter = null;
        if (counters != null) {
            counter = counters.findCounter(group, name);
        }
        return counter;
    }
    public org.apache.hadoop.mapred.Counters.Counter getCounter(Enum<?> name) {
        return counters == null ? null : counters.findCounter(name);
    }
    public void incrCounter(Enum key, long amount) {
        if (counters != null) {
            counters.incrCounter(key, amount);
        }
        setProgressFlag();
    }
    public void incrCounter(String group, String counter, long amount) {
        if (counters != null) {
            counters.incrCounter(group, counter, amount);
        }
        if(skipping && SkipBadRecords.COUNTER_GROUP.equals(group) && (
                SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS.equals(counter) ||
                        SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS.equals(counter))) {
            //if application reports the processed records, move the
            //currentRecStartIndex to the next.
            //currentRecStartIndex is the start index which has not yet been
            //finished and is still in task's stomach.
            for(int i=0;i<amount;i++) {
                currentRecStartIndex = currentRecIndexIterator.next();
            }
        }
        setProgressFlag();
    }
    public void setInputSplit(org.apache.hadoop.mapred.InputSplit split) {
        this.split = split;
    }
    public InputSplit getInputSplit() throws UnsupportedOperationException {
        if (split == null) {
            throw new UnsupportedOperationException("Input only available on map");
        } else {
            return split;
        }
    }

    void resetDoneFlag() {
        synchronized(lock) {
            done = true;
            lock.notify();
        }
    }

    public void startCommunicationThread() {
        if (pingThread == null) {
            pingThread = new Thread(this, "communication thread");
            pingThread.setDaemon(true);
            pingThread.start();
        }
    }
    public void stopCommunicationThread() throws InterruptedException {
        // Updating resources specified in ResourceCalculatorPlugin
        if (pingThread != null) {
            synchronized(lock) {
                lock.notify(); // wake up the wait in the while loop
                while(!done) {
                    lock.wait();
                }
            }
            pingThread.interrupt();
            pingThread.join();
        }
    }
}
