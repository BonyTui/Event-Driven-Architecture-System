package tributary;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

// import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import tributary.api.TributaryService;
import tributary.core.TributaryCluster;

public class TributaryTest {
    private TributaryService tributary;

    @BeforeEach
    public void initialize() {
        tributary = new TributaryCluster();
    }

    @AfterEach
    public void clear() {
        tributary.clearTributaryCluster();
    }

    @Test
    public void createTopic() {
        String topicId = "topic1";
        String topicType = "type1";
        tributary.createTopic(topicId, topicType);
        tributary.showTopic(topicId);
    }

    @Test
    public void createTopics() {

    }

    @Test
    public void createPartition() {

    }

    @Test
    public void createPartitions() {

    }

    @Test
    public void createProducer() {

    }

    @Test
    public void createProducers() {

    }

    @Test
    public void createConsumer() {

    }

    @Test
    public void createConsumers() {

    }

    @Test
    public void createConsumerGroup() {

    }

    @Test
    public void createConsumerGroups() {

    }
}
