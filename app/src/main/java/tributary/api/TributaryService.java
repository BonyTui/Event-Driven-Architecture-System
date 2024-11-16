package tributary.api;

import java.util.List;
import tributary.core.ConsumerGroup;
import tributary.core.Event;
import tributary.core.Partition;
import tributary.core.Producer;
import tributary.core.Topic;
import tributary.core.Consumer;

public interface TributaryService {
    ///////////////////// Create commands /////////////////////

    // Creates a new topic
    public Topic createTopic(String id, String type);

    // Adds a partition to a topic
    public Partition createPartition(String topicId, String partitionId);

    // Creates a consumer group
    public ConsumerGroup createConsumerGroup(String groupId, String topicId, String strategy);

    // Adds a consumer to a group
    public Consumer createConsumer(String groupId, String consumerId);

    // Creates a producer with a specified allocation
    public Producer createProducer(String producerId, String type, String allocation);

    ///////////////////// Delete commands /////////////////////

    // Clear the entire datastore
    public void clearTributaryCluster();

    // Deletes a consumer and returns rebalanced group info
    public void deleteConsumer(String consumerId);

    // Produce and consume commands
    public Event produceEvent(String producerId, String topicId, String eventContent, String partitionId);

    // Consumes a single event from a partition
    public Event consumeEvent(String consumerId, String partitionId);

    // Consumes multiple events
    public List<Event> consumeEvents(String consumerId, String partitionId, int numberOfEvents);

    ///////////////////// Show commands /////////////////////

    // Show datastore, containing producers, topics, consuemrs
    public void showAll();

    // Displays topic info with partitions and events
    public String showTopic(String topicId);

    // Displays consumers in a group and their partitions
    public String showConsumerGroup(String groupId);

    // Displays consumers in a group and their partitions
    public String showProducer(String groupId);

    // // Parallel commands
    // Map<String, String> parallelProduce(List<ProduceRequest> requests); // Publishes multiple events in parallel

    // Map<String, Event> parallelConsume(List<ConsumeRequest> requests); // Consumes multiple events in parallel

    // Rebalancing and playback

    // Sets rebalancing method for a group
    public ConsumerGroup setConsumerGroupRebalancing(String consumerGroupId, String balancingMethod);

    // List<Event> playback(String consumerId, String partitionId, int offset);
    // Plays back events from a specific offset
}
