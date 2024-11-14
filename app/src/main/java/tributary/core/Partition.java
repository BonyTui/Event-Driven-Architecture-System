package tributary.core;

import java.util.PriorityQueue;
import java.util.Queue;

public class Partition {
    private String partitionId;
    private Queue<Event> eventQueue = new PriorityQueue<Event>();

    public Partition(String partitionId) {
        this.partitionId = partitionId;
    }

    public Queue<Event> getMessageQueue() {
        return eventQueue;
    }

    public String getPartitionId() {
        return partitionId;
    }
}
