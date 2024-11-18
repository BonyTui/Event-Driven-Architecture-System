package tributary.core;

import java.util.ArrayList;
import java.util.List;

public class ConsumerGroup<T> {
    private String consumerGroupId;
    private String topicId;
    private String balancingMethod;
    private List<Consumer<T>> consumerList = new ArrayList<>();

    public ConsumerGroup(String consumerGroupId, String topicId, String balancingMethod) {
        this.consumerGroupId = consumerGroupId;
        this.topicId = topicId;
        this.balancingMethod = balancingMethod;
    }

    public void addConsumer(Consumer<T> c) {
        getConsumerList().add(c);
    }

    public List<Consumer<T>> getConsumerList() {
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

    public void setBalancingMethod(String balancingMethod) {
        this.balancingMethod = balancingMethod;
    }

}
