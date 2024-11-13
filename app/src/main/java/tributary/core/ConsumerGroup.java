package tributary.core;

import java.util.ArrayList;
import java.util.List;

public class ConsumerGroup {
    private List<Consumer> consumerList = new ArrayList<>();
    private String consumerGroupId;
    private String topicId;
    private String balancingMethod;

    public ConsumerGroup(String consumerGroupId, String topicId, String balancingMethod) {
        this.consumerGroupId = consumerGroupId;
        this.topicId = topicId;
        this.balancingMethod = balancingMethod;
    }

    public void addConsumer(Consumer c) {
        getConsumerList().add(c);
    }

    public List<Consumer> getConsumerList() {
        return consumerList;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public String getTopicId() {
        return topicId;
    }

    public String getBalancingMethod() {
        return balancingMethod;
    }

}
