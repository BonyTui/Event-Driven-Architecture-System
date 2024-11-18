package tributary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import tributary.api.TributaryService;
import tributary.core.Consumer;
import tributary.core.ConsumerGroup;
import tributary.core.Event;
import tributary.core.Partition;
import tributary.core.Producer;
import tributary.core.Topic;
import tributary.core.TributaryCluster;

public class TributaryTest {
    private TributaryService<String> tributary;
    private ObjectMapper mapper; // Pretty Printer

    @BeforeEach
    public void initialize() {
        tributary = new TributaryCluster<>();
        mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @AfterEach
    public void clear() {
        tributary.clearTributaryCluster();
    }

    // Unit Tests
    @Test
    public void createTopic() {
        String topicId = "topic1";
        String topicType = "type1";
        Topic<String> t = tributary.createTopic(topicId, topicType);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(t);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showTopic(topicId);

        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void createInvalidTopic() {
        String topicId = "topic1";
        String topicType = "type1";
        tributary.createTopic(topicId, topicType);
        Topic<String> duplicatedTopic = tributary.createTopic(topicId, topicType);

        assertEquals(duplicatedTopic, null);
    }

    @Test
    public void createTopics() {
        String topicId = "topic1";
        String topicId2 = "topic2";
        String topicId3 = "topic3";
        String topicType = "type1";
        String topicType2 = "type2";
        String topicType3 = "type3";
        Topic<String> t = tributary.createTopic(topicId, topicType);
        Topic<String> t2 = tributary.createTopic(topicId2, topicType2);
        Topic<String> t3 = tributary.createTopic(topicId3, topicType3);

        String expectedResult;
        String expectedResult2;
        String expectedResult3;
        try {
            expectedResult = mapper.writeValueAsString(t);
            expectedResult2 = mapper.writeValueAsString(t2);
            expectedResult3 = mapper.writeValueAsString(t3);
        } catch (JsonProcessingException e) {
            expectedResult = null;
            expectedResult2 = null;
            expectedResult3 = null;
        }

        String actualResult = tributary.showTopic(topicId);
        String actualResult2 = tributary.showTopic(topicId2);
        String actualResult3 = tributary.showTopic(topicId3);

        assertEquals(expectedResult, actualResult);
        assertEquals(expectedResult2, actualResult2);
        assertEquals(expectedResult3, actualResult3);
    }

    @Test
    public void createPartition() {
        String topicId = "topic1";
        String topicType = "type1";
        Topic<String> t = tributary.createTopic(topicId, topicType);

        String partitionId = "partition1";
        Partition<String> p = tributary.createPartition(topicId, partitionId);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(t);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showTopic(topicId);

        assertEquals(expectedResult, actualResult);
        assertNotEquals(null, p);
    }

    @Test
    public void createInvalidPartition() {
        String topicId = "topic1";
        String topicType = "type1";
        Topic<String> t = tributary.createTopic(topicId, topicType);

        String partitionId = "partition1";
        String invalidTopicId = "topic10";
        Partition<String> p = tributary.createPartition(invalidTopicId, partitionId);
        Partition<String> validPartition = tributary.createPartition(topicId, partitionId);
        Partition<String> duplicatedPartition = tributary.createPartition(topicId, partitionId);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(t);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showTopic(topicId);

        assertEquals(expectedResult, actualResult);
        assertEquals(null, p);
        assertNotEquals(validPartition, null);
        assertEquals(duplicatedPartition, null);
    }

    @Test
    public void createPartitions() {
        String topicId = "topic1";
        String topicType = "type1";
        Topic<String> t = tributary.createTopic(topicId, topicType);

        String partitionId = "partition1";
        Partition<String> p = tributary.createPartition(topicId, partitionId);
        String partitionId2 = "partition2";
        Partition<String> p2 = tributary.createPartition(topicId, partitionId2);
        String partitionId3 = "partition3";
        Partition<String> p3 = tributary.createPartition(topicId, partitionId3);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(t);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showTopic(topicId);

        assertEquals(expectedResult, actualResult);
        assertNotEquals(null, p);
        assertNotEquals(null, p2);
        assertNotEquals(null, p3);

    }

    @Test
    public void createProducer() {
        String producerId = "producer1";
        String topicType = "type1";
        String allocationType = "Random";
        Producer<String> p = tributary.createProducer(producerId, topicType, allocationType);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(p);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showProducer(producerId);

        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void createInvalidProducer() {
        String producerId = "producer1";
        String producerId2 = "producer2";
        String topicType = "type1";
        String allocationType = "Random";
        String invalidAllocationType = "InvalidAllocationType";
        Producer<String> p = tributary.createProducer(producerId, topicType, invalidAllocationType);
        Producer<String> validProducer = tributary.createProducer(producerId2, topicType, allocationType);
        Producer<String> duplicatedProducer = tributary.createProducer(producerId2, topicType, allocationType);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(p);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showProducer(producerId);

        assertEquals(expectedResult, actualResult);
        assertEquals(null, p);
        assertEquals(null, duplicatedProducer);
        assertNotEquals(null, validProducer);
    }

    @Test
    public void createProducers() {
        String producerId = "producer1";
        String topicType = "type1";
        String allocationType = "Random";

        String producerId2 = "producer2";
        String topicType2 = "type1";
        String allocationType2 = "Manual";

        String producerId3 = "producer3";
        String topicType3 = "type3";
        String allocationType3 = "Random";

        Producer<String> p = tributary.createProducer(producerId, topicType, allocationType);
        Producer<String> p2 = tributary.createProducer(producerId2, topicType2, allocationType2);
        Producer<String> p3 = tributary.createProducer(producerId3, topicType3, allocationType3);

        String expectedResult;
        String expectedResult2;
        String expectedResult3;
        try {
            expectedResult = mapper.writeValueAsString(p);
            expectedResult2 = mapper.writeValueAsString(p2);
            expectedResult3 = mapper.writeValueAsString(p3);
        } catch (JsonProcessingException e) {
            expectedResult = null;
            expectedResult2 = null;
            expectedResult3 = null;
        }

        String actualResult = tributary.showProducer(producerId);
        String actualResult2 = tributary.showProducer(producerId2);
        String actualResult3 = tributary.showProducer(producerId3);

        assertEquals(expectedResult, actualResult);
        assertEquals(expectedResult2, actualResult2);
        assertEquals(expectedResult3, actualResult3);
    }

    @Test
    public void createConsumerGroup() {
        String consumerGroupId = "consumer_group1";
        String topicType = "type1";
        String balancingStrategy = "RoundRobin";
        ConsumerGroup<String> cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(cg);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showConsumerGroup(consumerGroupId);

        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void createInvalidConsumerGroup() {
        String consumerGroupId = "consumer_group1";
        String consumerGroupId2 = "consumer_group2";
        String topicType = "type1";
        String validBalancingStrategy = "RoundRobin";
        String invalidBalancingStrategy = "InvalidBalancingStrategy";
        ConsumerGroup<String> cg = tributary.createConsumerGroup(consumerGroupId, topicType, invalidBalancingStrategy);
        ConsumerGroup<String> validCg = tributary.createConsumerGroup(consumerGroupId2, topicType,
                validBalancingStrategy);
        ConsumerGroup<String> dupCg = tributary.createConsumerGroup(consumerGroupId2, topicType,
                validBalancingStrategy);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(cg);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showConsumerGroup(consumerGroupId);

        assertEquals(expectedResult, actualResult);
        assertEquals(cg, null);
        assertNotEquals(validCg, null);
        assertEquals(dupCg, null);
    }

    @Test
    public void createConsumerGroups() {
        String consumerGroupId = "consumer_group1";
        String topicType = "type1";
        String balancingStrategy = "RoundRobin";
        ConsumerGroup<String> cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

        String consumerGroupId2 = "consumer_group2";
        String topicType2 = "type1";
        String balancingStrategy2 = "Range";
        ConsumerGroup<String> cg2 = tributary.createConsumerGroup(consumerGroupId2, topicType2, balancingStrategy2);

        String consumerGroupId3 = "consumer_group3";
        String topicType3 = "type9";
        String balancingStrategy3 = "RoundRobin";
        ConsumerGroup<String> cg3 = tributary.createConsumerGroup(consumerGroupId3, topicType3, balancingStrategy3);

        String expectedResult;
        String expectedResult2;
        String expectedResult3;
        try {
            expectedResult = mapper.writeValueAsString(cg);
            expectedResult2 = mapper.writeValueAsString(cg2);
            expectedResult3 = mapper.writeValueAsString(cg3);
        } catch (JsonProcessingException e) {
            expectedResult = null;
            expectedResult2 = null;
            expectedResult3 = null;
        }

        String actualResult = tributary.showConsumerGroup(consumerGroupId);
        String actualResult2 = tributary.showConsumerGroup(consumerGroupId2);
        String actualResult3 = tributary.showConsumerGroup(consumerGroupId3);

        assertEquals(expectedResult, actualResult);
        assertEquals(expectedResult2, actualResult2);
        assertEquals(expectedResult3, actualResult3);
    }

    @Test
    public void createConsumer() {
        String consumerGroupId = "consumer_group1";
        String topicType = "type1";
        String balancingStrategy = "RoundRobin";
        ConsumerGroup<String> cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

        String consumerId = "consumer1";
        Consumer<String> c = tributary.createConsumer(consumerGroupId, consumerId);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(cg);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showConsumerGroup(consumerGroupId);

        assertEquals(expectedResult, actualResult);
        assertNotEquals(c, null);
    }

    @Test
    public void createInvalidConsumer() {
        String consumerGroupId = "consumer_group1";
        String topicType = "type1";
        String balancingStrategy = "RoundRobin";
        ConsumerGroup<String> cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

        String consumerId = "consumer1";
        String invalidConsumerGroupId = "consumer_group10";
        Consumer<String> c = tributary.createConsumer(invalidConsumerGroupId, consumerId);
        Consumer<String> validConsumer = tributary.createConsumer(consumerGroupId, consumerId);
        Consumer<String> dupConsumer = tributary.createConsumer(consumerGroupId, consumerId);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(cg);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showConsumerGroup(consumerGroupId);

        assertEquals(expectedResult, actualResult);
        assertEquals(c, null);
        assertNotEquals(validConsumer, null);
        assertEquals(dupConsumer, null);
    }

    @Test
    public void createConsumers() {
        String consumerGroupId = "consumer_group1";
        String topicType = "type1";
        String balancingStrategy = "RoundRobin";
        ConsumerGroup<String> cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

        String consumerId = "consumer1";
        Consumer<String> c = tributary.createConsumer(consumerGroupId, consumerId);

        String consumerId2 = "consumer2";
        Consumer<String> c2 = tributary.createConsumer(consumerGroupId, consumerId2);

        String consumerId3 = "consumer3";
        Consumer<String> c3 = tributary.createConsumer(consumerGroupId, consumerId3);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(cg);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showConsumerGroup(consumerGroupId);

        assertEquals(expectedResult, actualResult);
        assertNotEquals(c, null);
        assertNotEquals(c2, null);
        assertNotEquals(c3, null);
    }

    @Test
    public void deleteConsumer() {
        String consumerGroupId = "consumer_group1";
        String topicType = "type1";
        String balancingStrategy = "RoundRobin";
        ConsumerGroup<String> cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

        String consumerId = "consumer1";
        tributary.createConsumer(consumerGroupId, consumerId);

        tributary.deleteConsumer(consumerId);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(cg);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showConsumerGroup(consumerGroupId);

        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void deleteInvalidConsumer() {
        String consumerGroupId = "consumer_group1";
        String topicType = "type1";
        String balancingStrategy = "RoundRobin";
        ConsumerGroup<String> cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

        String consumerId = "consumer1";
        tributary.createConsumer(consumerGroupId, consumerId);

        String invalidConsumerId = "consumer10";
        tributary.deleteConsumer(invalidConsumerId);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(cg);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showConsumerGroup(consumerGroupId);

        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void deleteConsumers() {
        String consumerGroupId = "consumer_group1";
        String topicType = "type1";
        String balancingStrategy = "RoundRobin";
        ConsumerGroup<String> cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

        String consumerId = "consumer1";
        tributary.createConsumer(consumerGroupId, consumerId);

        String consumerId2 = "consumer2";
        tributary.createConsumer(consumerGroupId, consumerId2);

        String consumerId3 = "consumer3";
        tributary.createConsumer(consumerGroupId, consumerId3);

        tributary.deleteConsumer(consumerId);
        tributary.deleteConsumer(consumerId2);
        tributary.deleteConsumer(consumerId3);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(cg);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showConsumerGroup(consumerGroupId);

        assertEquals(expectedResult, actualResult);
    }

    // Integration Tests
    @Test
    public void createAll() {
        // Step 1: Create Topics
        String topicId1 = "topic1";
        String topicId2 = "topic2";
        String topicType1 = "type1";
        String topicType2 = "type2";
        Topic<String> topic1 = tributary.createTopic(topicId1, topicType1);
        Topic<String> topic2 = tributary.createTopic(topicId2, topicType2);

        // Step 2: Create Partitions for the Topics
        String partitionId1 = "partition1";
        String partitionId2 = "partition2";
        Partition<String> partition1 = tributary.createPartition(topicId1, partitionId1);
        Partition<String> partition2 = tributary.createPartition(topicId2, partitionId2);

        // Step 3: Create Producers and Assign them to Topics
        String producerId1 = "producer1";
        String producerId2 = "producer2";
        String allocationType = "Random";
        Producer<String> producer1 = tributary.createProducer(producerId1, topicType1, allocationType);
        Producer<String> producer2 = tributary.createProducer(producerId2, topicType2, allocationType);

        // Step 4: Create Consumer<String> Groups for the Topics
        String consumerGroupId1 = "consumer_group1";
        String consumerGroupId2 = "consumer_group2";
        String balancingStrategy1 = "RoundRobin";
        String balancingStrategy2 = "Range";
        ConsumerGroup<String> consumerGroup1 = tributary.createConsumerGroup(consumerGroupId1, topicType1,
                balancingStrategy1);
        ConsumerGroup<String> consumerGroup2 = tributary.createConsumerGroup(consumerGroupId2, topicType2,
                balancingStrategy2);

        // Step 5: Create Consumers in Each Consumer<String> Group
        String consumerId1 = "consumer1";
        String consumerId2 = "consumer2";
        Consumer<String> consumer1 = tributary.createConsumer(consumerGroupId1, consumerId1);
        Consumer<String> consumer2 = tributary.createConsumer(consumerGroupId2, consumerId2);

        // Expected Results using Pretty Print for JSON Serialization
        String expectedTopic1;
        String expectedTopic2;
        String expectedProducer1;
        String expectedProducer2;
        String expectedConsumerGroup1;
        String expectedConsumerGroup2;

        try {
            expectedTopic1 = mapper.writeValueAsString(topic1);
            expectedTopic2 = mapper.writeValueAsString(topic2);
            expectedProducer1 = mapper.writeValueAsString(producer1);
            expectedProducer2 = mapper.writeValueAsString(producer2);
            expectedConsumerGroup1 = mapper.writeValueAsString(consumerGroup1);
            expectedConsumerGroup2 = mapper.writeValueAsString(consumerGroup2);
        } catch (JsonProcessingException e) {
            expectedTopic1 = expectedTopic2 = null;
            expectedProducer1 = expectedProducer2 = null;
            expectedConsumerGroup1 = expectedConsumerGroup2 = null;
        }

        // Actual Results
        String actualTopic1 = tributary.showTopic(topicId1);
        String actualTopic2 = tributary.showTopic(topicId2);
        String actualProducer1 = tributary.showProducer(producerId1);
        String actualProducer2 = tributary.showProducer(producerId2);
        String actualConsumerGroup1 = tributary.showConsumerGroup(consumerGroupId1);
        String actualConsumerGroup2 = tributary.showConsumerGroup(consumerGroupId2);

        // Assertions
        assertEquals(expectedTopic1, actualTopic1);
        assertEquals(expectedTopic2, actualTopic2);
        assertEquals(expectedProducer1, actualProducer1);
        assertEquals(expectedProducer2, actualProducer2);
        assertEquals(expectedConsumerGroup1, actualConsumerGroup1);
        assertEquals(expectedConsumerGroup2, actualConsumerGroup2);
        assertNotEquals(null, partition1);
        assertNotEquals(null, partition2);
        assertNotEquals(null, consumer1);
        assertNotEquals(null, consumer2);
    }

    @Test
    public void produceEventManual() throws IOException {
        String producerId = "producer1";
        String topicType = "String";
        String allocationType = "Manual";
        tributary.createProducer(producerId, topicType, allocationType);

        String topicId = "topic1";
        Topic<String> t = tributary.createTopic(topicId, topicType);

        String partitionId = "partition1";
        Partition<String> partition = tributary.createPartition(topicId, partitionId);

        // JSON file containing the event content
        String eventContentFilePath = "tributary/events/stringEvent.json";

        Event<String> event = tributary.produceEvent(producerId, topicId, eventContentFilePath, partitionId,
                String.class);

        String expectedResult = mapper.writeValueAsString(t);
        String actualResult = tributary.showTopic(topicId);

        assertEquals(expectedResult, actualResult);
        assertNotEquals(t, null);
        assertNotEquals(partition, null);
        assertNotEquals(event, null);
    }

    @Test
    public void produceEventRandom() throws IOException {
        String producerId = "producer1";
        String topicType = "Integer";
        String allocationType = "Random";
        tributary.createProducer(producerId, topicType, allocationType);

        String topicId = "topic1";
        Topic<String> t = tributary.createTopic(topicId, topicType);

        String partitionId = "partition1";
        Partition<String> partition = tributary.createPartition(topicId, partitionId);

        String eventContentFilePath = "tributary/events/stringEvent.json";

        Event<String> event = tributary.produceEvent(producerId, topicId, eventContentFilePath, partitionId,
                String.class);

        String expectedResult = mapper.writeValueAsString(t);
        String actualResult = tributary.showTopic(topicId);

        assertEquals(expectedResult, actualResult);
        assertNotEquals(t, null);
        assertNotEquals(partition, null);
        assertNotEquals(event, null);
    }

    @Test
    public void produceInvalidEvent() throws IOException {
        String producerId = "producer1";
        String topicType = "String";
        String allocationType = "Manual";
        tributary.createProducer(producerId, topicType, allocationType);

        String topicId = "topic1";
        tributary.createTopic(topicId, topicType);

        String partitionId = "partition1";
        tributary.createPartition(topicId, partitionId);

        // JSON file containing invalid event content
        String eventContentFile = "path/to/invalidEvent.json";

        Event<String> event = tributary.produceEvent("invalidProducer", topicId, eventContentFile, partitionId,
                String.class);
        assertNull(event);

        event = tributary.produceEvent(producerId, "invalidTopic", eventContentFile, partitionId, String.class);
        assertNull(event);

        event = tributary.produceEvent(producerId, topicId, eventContentFile, "invalidPartition", String.class);
        assertNull(event);
    }

    @Test
    public void produceEventsManual() throws IOException {
        String producerId = "producer1";
        String topicType = "String";
        String allocationType = "Manual";
        tributary.createProducer(producerId, topicType, allocationType);

        String topicId = "topic1";
        Topic<String> topic = tributary.createTopic(topicId, topicType);

        String partitionId = "partition1";
        Partition<String> partition = tributary.createPartition(topicId, partitionId);

        // JSON files containing the event content
        String eventContentFile1 = getClass().getClassLoader().getResource("tributary/events/stringEvent.json")
                .getPath();
        String eventContentFile2 = getClass().getClassLoader().getResource("tributary/events/intEvent2.json").getPath();

        Event<String> event1 = tributary.produceEvent(producerId, topicId, eventContentFile1, partitionId,
                String.class);
        Event<String> event2 = tributary.produceEvent(producerId, topicId, eventContentFile2, partitionId,
                String.class);

        String expectedResult = mapper.writeValueAsString(topic);
        String actualResult = tributary.showTopic(topicId);

        assertEquals(expectedResult, actualResult);
        assertNotEquals(topic, null);
        assertNotEquals(partition, null);
        assertNotEquals(event1, null);
        assertNotEquals(event2, null);
    }

    @Test
    public void produceEventsRandom() throws IOException {
        String producerId = "producer1";
        String topicType = "Integer";
        String allocationType = "Random";
        tributary.createProducer(producerId, topicType, allocationType);

        String topicId = "topic1";
        Topic<String> topic = tributary.createTopic(topicId, topicType);

        String partitionId1 = "partition1";
        String partitionId2 = "partition2";
        tributary.createPartition(topicId, partitionId1);
        tributary.createPartition(topicId, partitionId2);

        // JSON files containing event content
        String eventContentFile1 = getClass().getClassLoader().getResource("tributary/events/intEvent.json").getPath();
        String eventContentFile2 = getClass().getClassLoader().getResource("tributary/events/intEvent2.json").getPath();

        Event<String> event1 = tributary.produceEvent(producerId, topicId, eventContentFile1, null, String.class);
        Event<String> event2 = tributary.produceEvent(producerId, topicId, eventContentFile2, null, String.class);

        String expectedResult = mapper.writeValueAsString(topic);
        String actualResult = tributary.showTopic(topicId);

        assertEquals(expectedResult, actualResult);
        assertNotEquals(topic, null);
        assertNotEquals(event1, null);
        assertNotEquals(event2, null);
    }

    @Test
    public void consumeEvent() throws IOException {
        String producerId = "producer1";
        String topicType = "String";
        String allocationType = "Manual";
        tributary.createProducer(producerId, topicType, allocationType);

        String topicId = "topic1";
        tributary.createTopic(topicId, topicType);

        String partitionId = "partition1";
        tributary.createPartition(topicId, partitionId);

        // JSON file containing the event content
        String eventContentFilePath = "tributary/events/stringEvent.json";

        Event<String> producedEvent = tributary.produceEvent(producerId, topicId, eventContentFilePath, partitionId,
                String.class);

        String consumerGroupId = "consumerGroup1";
        tributary.createConsumerGroup(consumerGroupId, topicId, "RoundRobin");

        String consumerId = "consumer1";
        tributary.createConsumer(consumerGroupId, consumerId);

        Event<String> consumedEvent = tributary.consumeEvent(consumerId, partitionId);

        assertEquals(producedEvent, consumedEvent);
    }

    @Test
    public void consumeInvalidEvent() {
        String producerId = "producer1";
        String topicType = "type1";
        String allocationType = "Random";
        tributary.createProducer(producerId, topicType, allocationType);

        String topicId = "topic1";
        tributary.createTopic(topicId, topicType);

        String partitionId = "partition1";
        tributary.createPartition(topicId, partitionId);

        String eventContent = "event1";
        tributary.produceEvent(producerId, topicId, eventContent, partitionId, String.class);

        String consumerGroupId = "consumerGroup1";
        tributary.createConsumerGroup(consumerGroupId, topicId, "RoundRobin");

        String consumerId = "consumer1";
        tributary.createConsumer(consumerGroupId, consumerId);

        // Test invalid partition
        Event<String> consumedEvent = tributary.consumeEvent(consumerId, "invalidPartition");
        assertEquals(null, consumedEvent);

        // Test invalid consumer
        consumedEvent = tributary.consumeEvent("invalidConsumer", partitionId);
        assertEquals(null, consumedEvent);
    }

    @Test
    public void consumeEvents() throws IOException {
        String producerId = "producer1";
        String topicType = "String";
        String allocationType = "Manual";
        tributary.createProducer(producerId, topicType, allocationType);

        String topicId = "topic1";
        tributary.createTopic(topicId, topicType);

        String partitionId = "partition1";
        tributary.createPartition(topicId, partitionId);

        // JSON files containing event content
        String eventContentFile1 = "tributary/events/stringEvent.json";
        String eventContentFile2 = "tributary/events/stringEvent2.json";
        String eventContentFile3 = "tributary/events/intEvent2.json";

        Event<String> producedEvent1 = tributary.produceEvent(producerId, topicId, eventContentFile1, partitionId,
                String.class);
        Event<String> producedEvent2 = tributary.produceEvent(producerId, topicId, eventContentFile2, partitionId,
                String.class);
        Event<String> producedEvent3 = tributary.produceEvent(producerId, topicId, eventContentFile3, partitionId,
                String.class);

        String consumerGroupId = "consumerGroup1";
        tributary.createConsumerGroup(consumerGroupId, topicId, "RoundRobin");

        String consumerId = "consumer1";
        tributary.createConsumer(consumerGroupId, consumerId);

        List<Event<String>> consumedEvents = tributary.consumeEvents(consumerId, partitionId, 3);

        assertEquals(3, consumedEvents.size());
        assertEquals(producedEvent1, consumedEvents.get(0));
        assertEquals(producedEvent2, consumedEvents.get(1));
        assertEquals(producedEvent3, consumedEvents.get(2));
    }

    @Test
    public void setValidConsumerGroupRebalancing() {
        // Initialize identifiers and balancing methods
        String consumerGroupId = "consumer_group1";
        String topicType = "type1";
        String initialBalancingMethod = "RoundRobin";
        String newBalancingMethod = "Range";

        // Create a consumer group with an initial balancing method
        tributary.createConsumerGroup(consumerGroupId, topicType, initialBalancingMethod);

        // Update the balancing method
        ConsumerGroup<String> updatedConsumerGroup = tributary.setConsumerGroupRebalancing(consumerGroupId,
                newBalancingMethod);

        // Assertions
        assertNotNull(updatedConsumerGroup);
        assertEquals(newBalancingMethod, updatedConsumerGroup.getBalancingMethod());
    }

    @Test
    public void setInvalidConsumerGroupRebalancing() {
        // Invalid consumer group ID
        String invalidConsumerGroupId = "invalid_group";
        String validBalancingMethod = "Range";

        // Invalid balancing method
        String consumerGroupId = "consumer_group2";
        String topicType = "type1";
        tributary.createConsumerGroup(consumerGroupId, topicType, "RoundRobin");
        String invalidBalancingMethod = "InvalidMethod";

        // Test invalid consumer group ID
        ConsumerGroup<String> resultForInvalidGroup = tributary.setConsumerGroupRebalancing(invalidConsumerGroupId,
                validBalancingMethod);
        assertNull(resultForInvalidGroup);

        // Test invalid balancing method
        ConsumerGroup<String> resultForInvalidMethod = tributary.setConsumerGroupRebalancing(consumerGroupId,
                invalidBalancingMethod);
        assertNull(resultForInvalidMethod);
    }

    @Test
    public void testParallelProduce() {
        String topicId = "topic1";
        String topicType = "String";
        Topic<String> topic = tributary.createTopic(topicId, topicType);

        String producerId = "producer1";
        String allocationType = "Random";

        String producerId2 = "producer2";
        String allocationType2 = "Manual";

        Producer<String> p = tributary.createProducer(producerId, topicType, allocationType);
        Producer<String> p2 = tributary.createProducer(producerId2, topicType, allocationType2);

        String partitionId = "partition1";
        tributary.createPartition(topicId, partitionId);

        // Create events
        // JSON file containing the event content
        String eventContentFilePath = "tributary/events/stringEvent.json";

        Event<String> event1 = tributary.produceEvent(producerId2, topicId, eventContentFilePath, partitionId,
                String.class);
        // JSON file containing the event content
        String eventContentFilePath2 = "tributary/events/stringEvent2.json";

        Event<String> event2 = tributary.produceEvent(producerId, topicId, eventContentFilePath2, partitionId,
                String.class);

        List<Producer<String>> producers = Arrays.asList(p, p2);
        List<Event<String>> events = Arrays.asList(event1, event2);

        // Call parallelProduce
        tributary.parallelProduce(producers, topicId, events);

        // Verify events in topic's partitions
        List<Partition<String>> partitions = topic.getPartitionList();
        assertFalse(partitions.isEmpty());

        List<String> producedEventIds = new ArrayList<>();
        for (Partition<String> pp : partitions) {
            for (Event<String> event : pp.getEventQueue()) {
                producedEventIds.add(event.getEventId());
            }
        }

        assertTrue(!topic.getPartitionList().isEmpty());
    }

    @Test
    public void testParallelConsume() {
        String topicId = "topic1";
        String topicType = "String";
        Topic<String> topic = tributary.createTopic(topicId, topicType);

        String consumerId1 = "consumer1";
        String consumerId2 = "consumer2";

        Consumer<String> c1 = tributary.createConsumer(consumerId1, topicType);
        Consumer<String> c2 = tributary.createConsumer(consumerId2, topicType);

        String partitionId = "partition1";
        tributary.createPartition(topicId, partitionId);

        // Add events to partition
        String eventContentFilePath = "tributary/events/stringEvent.json";
        Event<String> event1 = tributary.produceEvent("producer1", topicId, eventContentFilePath, partitionId,
                String.class);

        String eventContentFilePath2 = "tributary/events/stringEvent2.json";
        Event<String> event2 = tributary.produceEvent("producer2", topicId, eventContentFilePath2, partitionId,
                String.class);

        // Add partition to topic
        Partition<String> partition = topic.getPartitionList().get(0);
        partition.getEventQueue().add(event1);
        partition.getEventQueue().add(event2);

        List<Consumer<String>> consumers = Arrays.asList(c1, c2);
        List<Partition<String>> partitions = topic.getPartitionList();

        // Call parallelConsume
        tributary.parallelConsume(consumers, partitions);

        // Verify consumption
        for (Partition<String> p : partitions) {
            assertTrue(!p.getEventQueue().isEmpty());
        }
    }
}
