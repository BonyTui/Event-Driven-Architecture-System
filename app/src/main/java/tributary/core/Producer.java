package tributary.core;

import java.util.List;

public abstract class Producer<T> {
    private String producerId;
    private String topicType;

    public Producer(String producerId, String topicType) {
        this.producerId = producerId;
        this.topicType = topicType;
    }

    public String getProducerId() {
        return producerId;
    }

    public String getTopicType() {
        return topicType;
    }

    public String getType() {
        return topicType;
    }

    public abstract boolean assignEvent(Event<T> event, List<Partition<T>> partitionList, String partitionId);
}
