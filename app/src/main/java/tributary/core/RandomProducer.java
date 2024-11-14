package tributary.core;

import java.util.List;

public class RandomProducer extends Producer {
    public RandomProducer(String producerId, String topicType) {
        super(producerId, topicType);
    }

    /**
     * @pre: partitionId is null, cause assign randomly
     * @post: event gets added to random partition
     */
    public void assignEvent(Event event, List<Partition> partitionList, String partitionId) {
        int randomIndex = (int) Math.floor(Math.random() * partitionList.size());
        partitionList.get(randomIndex).getEventQueue().add(event);
    }

}
