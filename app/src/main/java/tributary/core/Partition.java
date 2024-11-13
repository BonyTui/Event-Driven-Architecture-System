package tributary.core;

import java.util.PriorityQueue;
import java.util.Queue;

public class Partition {
    private Queue<Message> messageQueue = new PriorityQueue<Message>();
    private String id;

    public Partition(String id) {
        this.id = id;
    }

    public Queue<Message> getMessageQueue() {
        return messageQueue;
    }

    public String getId() {
        return id;
    }
}