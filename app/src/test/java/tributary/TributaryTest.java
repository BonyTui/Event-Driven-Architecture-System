package tributary;

import tributary.core.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.junit.jupiter.api.Test;

import tributary.api.TributaryService;

public class TributaryTest {
    private TributaryService tributary;
    private ObjectMapper mapper; // Pretty Printer

    @BeforeEach
    public void initialize() {
        tributary = new TributaryCluster();
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
        Topic t = tributary.createTopic(topicId, topicType);

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
    public void createTopics() {
        String topicId = "topic1";
        String topicId2 = "topic2";
        String topicId3 = "topic3";
        String topicType = "type1";
        String topicType2 = "type2";
        String topicType3 = "type3";
        Topic t = tributary.createTopic(topicId, topicType);
        Topic t2 = tributary.createTopic(topicId2, topicType2);
        Topic t3 = tributary.createTopic(topicId3, topicType3);

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
        Topic t = tributary.createTopic(topicId, topicType);

        String partitionId = "partition1";
        Partition p = tributary.createPartition(topicId, partitionId);

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
        Topic t = tributary.createTopic(topicId, topicType);

        String partitionId = "partition1";
        String invalidTopicId = "topic10";
        Partition p = tributary.createPartition(invalidTopicId, partitionId);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(t);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showTopic(topicId);

        assertEquals(expectedResult, actualResult);
        assertEquals(null, p);
    }

    @Test
    public void createPartitions() {
        String topicId = "topic1";
        String topicType = "type1";
        Topic t = tributary.createTopic(topicId, topicType);

        String partitionId = "partition1";
        Partition p = tributary.createPartition(topicId, partitionId);
        String partitionId2 = "partition2";
        Partition p2 = tributary.createPartition(topicId, partitionId2);
        String partitionId3 = "partition3";
        Partition p3 = tributary.createPartition(topicId, partitionId3);

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
        Producer p = tributary.createProducer(producerId, topicType, allocationType);

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
        String topicType = "type1";
        String allocationType = "InvalidAllocationType";
        Producer p = tributary.createProducer(producerId, topicType, allocationType);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(p);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showProducer(producerId);

        assertEquals(expectedResult, actualResult);
        assertEquals(null, p);
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

        Producer p = tributary.createProducer(producerId, topicType, allocationType);
        Producer p2 = tributary.createProducer(producerId2, topicType2, allocationType2);
        Producer p3 = tributary.createProducer(producerId3, topicType3, allocationType3);

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
        ConsumerGroup cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

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
        String topicType = "type1";
        String balancingStrategy = "InvalidBalancingStrategy";
        ConsumerGroup cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(cg);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showConsumerGroup(consumerGroupId);

        assertEquals(expectedResult, actualResult);
        assertEquals(cg, null);
    }

    @Test
    public void createConsumerGroups() {
        String consumerGroupId = "consumer_group1";
        String topicType = "type1";
        String balancingStrategy = "RoundRobin";
        ConsumerGroup cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

        String consumerGroupId2 = "consumer_group2";
        String topicType2 = "type1";
        String balancingStrategy2 = "Range";
        ConsumerGroup cg2 = tributary.createConsumerGroup(consumerGroupId2, topicType2, balancingStrategy2);

        String consumerGroupId3 = "consumer_group3";
        String topicType3 = "type9";
        String balancingStrategy3 = "RoundRobin";
        ConsumerGroup cg3 = tributary.createConsumerGroup(consumerGroupId3, topicType3, balancingStrategy3);

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
        ConsumerGroup cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

        String consumerId = "consumer1";
        Consumer c = tributary.createConsumer(consumerGroupId, consumerId);

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
        ConsumerGroup cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

        String consumerId = "consumer1";
        String invalidConsumerGroupId = "consumer_group10";
        Consumer c = tributary.createConsumer(invalidConsumerGroupId, consumerId);

        String expectedResult;
        try {
            expectedResult = mapper.writeValueAsString(cg);
        } catch (JsonProcessingException e) {
            expectedResult = null;
        }

        String actualResult = tributary.showConsumerGroup(consumerGroupId);

        assertEquals(expectedResult, actualResult);
        assertEquals(c, null);
    }

    @Test
    public void createConsumers() {
        String consumerGroupId = "consumer_group1";
        String topicType = "type1";
        String balancingStrategy = "RoundRobin";
        ConsumerGroup cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

        String consumerId = "consumer1";
        Consumer c = tributary.createConsumer(consumerGroupId, consumerId);

        String consumerId2 = "consumer2";
        Consumer c2 = tributary.createConsumer(consumerGroupId, consumerId2);

        String consumerId3 = "consumer3";
        Consumer c3 = tributary.createConsumer(consumerGroupId, consumerId3);

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
        ConsumerGroup cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

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
        ConsumerGroup cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

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
        ConsumerGroup cg = tributary.createConsumerGroup(consumerGroupId, topicType, balancingStrategy);

        String consumerId = "consumer1";
        Consumer c = tributary.createConsumer(consumerGroupId, consumerId);

        String consumerId2 = "consumer2";
        Consumer c2 = tributary.createConsumer(consumerGroupId, consumerId2);

        String consumerId3 = "consumer3";
        Consumer c3 = tributary.createConsumer(consumerGroupId, consumerId3);

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
        Topic topic1 = tributary.createTopic(topicId1, topicType1);
        Topic topic2 = tributary.createTopic(topicId2, topicType2);

        // Step 2: Create Partitions for the Topics
        String partitionId1 = "partition1";
        String partitionId2 = "partition2";
        Partition partition1 = tributary.createPartition(topicId1, partitionId1);
        Partition partition2 = tributary.createPartition(topicId2, partitionId2);

        // Step 3: Create Producers and Assign them to Topics
        String producerId1 = "producer1";
        String producerId2 = "producer2";
        String allocationType = "Random";
        Producer producer1 = tributary.createProducer(producerId1, topicType1, allocationType);
        Producer producer2 = tributary.createProducer(producerId2, topicType2, allocationType);

        // Step 4: Create Consumer Groups for the Topics
        String consumerGroupId1 = "consumer_group1";
        String consumerGroupId2 = "consumer_group2";
        String balancingStrategy1 = "RoundRobin";
        String balancingStrategy2 = "Range";
        ConsumerGroup consumerGroup1 = tributary.createConsumerGroup(consumerGroupId1, topicType1, balancingStrategy1);
        ConsumerGroup consumerGroup2 = tributary.createConsumerGroup(consumerGroupId2, topicType2, balancingStrategy2);

        // Step 5: Create Consumers in Each Consumer Group
        String consumerId1 = "consumer1";
        String consumerId2 = "consumer2";
        Consumer consumer1 = tributary.createConsumer(consumerGroupId1, consumerId1);
        Consumer consumer2 = tributary.createConsumer(consumerGroupId2, consumerId2);

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

}
