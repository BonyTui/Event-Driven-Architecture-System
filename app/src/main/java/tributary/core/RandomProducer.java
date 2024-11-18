package tributary.core;

import java.util.Random;
import java.util.List;

public class RandomProducer<T> extends Producer<T> {
    public RandomProducer(String producerId, String topicType) {
        super(producerId, topicType);
    }

    /**
     * @pre: partitionId is null, cause assign randomly
     * @post: event gets added to random partition
     */
    @Override
    public boolean assignEvent(Event<T> event, List<Partition<T>> partitionList, String partitionId) {
        Random rand = new Random();
        int randomIndex = rand.nextInt(partitionList.size());
        partitionList.get(randomIndex).getEventQueue().add(event);
        return true;
    }

}
