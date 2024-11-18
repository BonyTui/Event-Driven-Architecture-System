package tributary.core;

import java.util.List;
import java.util.ArrayList;

public class Consumer<T> {
    private String consumerId;
    private String consumerGroupId;
    private List<Event<T>> consumedEventList = new ArrayList<>();

    public Consumer(String consumerGroupId, String consumerId) {
        this.consumerGroupId = consumerGroupId;
        this.consumerId = consumerId;
    }

    public Event<T> consume(Partition<T> p) {
        Event<T> event = p.getEventQueue().remove();
        consumedEventList.add(event);
        return event;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public List<Event<T>> getConsumedEventList() {
        return consumedEventList;
    }
}
