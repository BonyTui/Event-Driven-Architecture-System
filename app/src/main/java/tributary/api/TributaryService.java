package tributary.api;

public interface TributaryService {
    // Create commands
    public void createTopic(String id, String type); // Creates a new topic

    public void createPartition(String topicId, String partitionId); // Adds a partition to a topic

    // ConsumerGroup createConsumerGroup(String groupId, String topicId, RebalancingStrategy strategy);
    // Creates a consumer group

    // Consumer createConsumer(String groupId, String consumerId); // Adds a consumer to a group

    // Producer createProducer(String producerId, Class<?> type, AllocationMethod allocation);
    // Creates a producer with a specified allocation

    // // Delete commands
    // String deleteConsumer(String consumerId); // Deletes a consumer and returns rebalanced group info

    // // Produce and consume commands
    // String produceEvent(String producerId, String topicId, String eventFile, String partitionId);
    // Publishes an event from JSON file

    // Event consumeEvent(String consumerId, String partitionId); // Consumes a single event from a partition

    // List<Event> consumeEvents(String consumerId, String partitionId, int numberOfEvents); // Consumes multiple events

    // // Show information commands
    public void showTopic(String topicId); // Displays topic info with partitions and events

    // String showConsumerGroup(String groupId); // Displays consumers in a group and their partitions

    // // Parallel commands
    // Map<String, String> parallelProduce(List<ProduceRequest> requests); // Publishes multiple events in parallel

    // Map<String, Event> parallelConsume(List<ConsumeRequest> requests); // Consumes multiple events in parallel

    // // Rebalancing and playback
    // String setConsumerGroupRebalancing(String groupId, RebalancingStrategy strategy);
    // Sets rebalancing method for a group

    // List<Event> playback(String consumerId, String partitionId, int offset);
    // Plays back events from a specific offset
}
