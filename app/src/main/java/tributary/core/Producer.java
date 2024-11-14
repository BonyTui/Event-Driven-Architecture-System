package tributary.core;

import java.util.List;

public abstract class Producer {
    private String producerId;
    private String topicType;

    public Producer(String producerId, String topicType) {
        this.producerId = producerId;
        this.topicType = topicType;
    }

    public String getProducerId() {
        return producerId;
    }

    public String getType() {
        return topicType;
    }

    public abstract void assignEvent(Event event, List<Partition> partitionList, String partitionId);
}
