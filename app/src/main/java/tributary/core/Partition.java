package tributary.core;

import java.util.PriorityQueue;
import java.util.Queue;

public class Partition {
    private Queue<Message> messageQueue = new PriorityQueue<Message>();
    private String partitionId;

    public Partition(String partitionId) {
        this.partitionId = partitionId;
    }

    public Queue<Message> getMessageQueue() {
        return messageQueue;
    }

    public String getId() {
        return partitionId;
    }
}
