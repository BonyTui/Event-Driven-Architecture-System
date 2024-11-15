package tributary.core;

import java.util.List;

public class ManualProducer extends Producer {
    public ManualProducer(String producerId, String topicType) {
        super(producerId, topicType);
    }

    /**
     * @pre: partitionId is valid
     * @post: event gets added to that partition
     */
    @Override
    public boolean assignEvent(Event event, List<Partition> partitionList, String partitionId) {
        Partition p = partitionList.stream().filter(partition -> partition.getPartitionId().equals(partitionId))
                .findAny().orElse(null);

        if (p == null) {
            return false;
        }

        p.getEventQueue().add(event);
        return true;
    }
}
