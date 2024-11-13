package tributary.api;

public interface TributaryService {
    // Create commands

    // Creates a new topic
    public void createTopic(String id, String type);

    // Adds a partition to a topic
    public void createPartition(String topicId, String partitionId);

    // Creates a consumer group
    public void createConsumerGroup(String groupId, String topicId, String strategy);

    // Adds a consumer to a group
    public void createConsumer(String groupId, String consumerId);

    // Creates a producer with a specified allocation
    public void createProducer(String producerId, String type, String allocation);

    // // Delete commands
    public void clearTributaryCluster();
    // String deleteConsumer(String consumerId); // Deletes a consumer and returns rebalanced group info

    // // Produce and consume commands
    // String produceEvent(String producerId, String topicId, String eventFile, String partitionId);
    // Publishes an event from JSON file

    // Event consumeEvent(String consumerId, String partitionId); // Consumes a single event from a partition

    // List<Event> consumeEvents(String consumerId, String partitionId, int numberOfEvents); // Consumes multiple events

    // Show information commands

    // Displays topic info with partitions and events
    public String showTopic(String topicId);

    // Show datastore, containing producers, topics, consuemrs
    public String showAll();

    // Displays consumers in a group and their partitions
    public String showConsumerGroup(String groupId);

    // Displays consumers in a group and their partitions
    public String showProducer(String groupId);

    // // Parallel commands
    // Map<String, String> parallelProduce(List<ProduceRequest> requests); // Publishes multiple events in parallel

    // Map<String, Event> parallelConsume(List<ConsumeRequest> requests); // Consumes multiple events in parallel

    // // Rebalancing and playback
    // String setConsumerGroupRebalancing(String groupId, RebalancingStrategy strategy);
    // Sets rebalancing method for a group

    // List<Event> playback(String consumerId, String partitionId, int offset);
    // Plays back events from a specific offset
}
