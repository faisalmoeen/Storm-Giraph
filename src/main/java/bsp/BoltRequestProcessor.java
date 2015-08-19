package bsp;

import java.util.HashMap;

/**
 * Created by faisalorakzai on 16/08/15.
 */
public class BoltRequestProcessor {
    private HashMap<Long,Long> messageMap;
    public BoltRequestProcessor() {
        messageMap = new HashMap<Long, Long>();
    }

    public void sendMessage(Long id, Long value) {
        messageMap.put(id,value);
    }

    public boolean hasMessages(Long superStep) {
        return (messageMap.size()>0?true:false);
    }
}
