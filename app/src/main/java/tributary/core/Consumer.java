package tributary.core;

public class Consumer {
    private String consumerId;
    private String consumerGroupId;
    private String partitionId;

    public Consumer(String consumerGroupId, String consumerId) {
        this.consumerGroupId = consumerGroupId;
        this.consumerId = consumerId;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }
}
