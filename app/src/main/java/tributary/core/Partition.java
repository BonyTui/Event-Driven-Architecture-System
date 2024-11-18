package tributary.core;

import java.util.Queue;
import java.util.LinkedList;

public class Partition<T> {
    private String partitionId;
    private Queue<Event<T>> eventQueue = new LinkedList<>();

    public Partition(String partitionId) {
        this.partitionId = partitionId;
    }

    public Queue<Event<T>> getEventQueue() {
        return eventQueue;
    }

    public String getPartitionId() {
        return partitionId;
    }
}
