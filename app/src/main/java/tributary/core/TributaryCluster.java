package tributary.core;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import tributary.api.TributaryService;

public class TributaryCluster implements TributaryService {
    private List<Producer> producerList = new ArrayList<>();
    private List<Topic> topicList = new ArrayList<>();
    private List<ConsumerGroup> consumerGroupList = new ArrayList<>();

    private List<String> validConsumerGroupBalancingMethods = new ArrayList<>();

    private ObjectMapper mapper; // Pretty Printer

    public TributaryCluster() {
        mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        validConsumerGroupBalancingMethods.add("Range");
        validConsumerGroupBalancingMethods.add("RoundRobin");
    }

    private void setProducerList(List<Producer> producerList) {
        this.producerList = producerList;
    }

    private void setTopicList(List<Topic> topicList) {
        this.topicList = topicList;
    }

    private void setConsumerGroupList(List<ConsumerGroup> consumerGroupList) {
        this.consumerGroupList = consumerGroupList;
    }

    public void clearTributaryCluster() {
        setProducerList(null);
        setTopicList(null);
        setConsumerGroupList(null);
    }

    private Topic findTopic(String id) {
        return topicList.stream().filter(topic -> topic.getTopicId().equals(id)).findAny().orElse(null);
    }

    private Partition findPartition(String id) {
        Partition p;
        for (Topic t : topicList) {
            p = t.getPartitionList().stream().filter(partition -> partition.getPartitionId().equals(id)).findAny()
                    .orElse(null);
            if (p != null) {
                return p;
            }
        }
        return null;
    }

    private Producer findProducer(String id) {
        return producerList.stream().filter(producer -> producer.getProducerId().equals(id)).findAny().orElse(null);
    }

    private ConsumerGroup findConsumerGroup(String id) {
        return consumerGroupList.stream().filter(group -> group.getConsumerGroupId().equals(id)).findAny().orElse(null);
    }

    private Consumer findConsumer(String id) {
        Consumer c;
        for (ConsumerGroup cg : consumerGroupList) {
            c = cg.getConsumerList().stream().filter(consumer -> consumer.getConsumerId().equals(id)).findAny()
                    .orElse(null);
            if (c != null) {
                return c;
            }
        }
        return null;
    }

    @Override
    public String showTopic(String id) {
        Topic t = findTopic(id);
        String prettyJson;
        try {
            prettyJson = mapper.writeValueAsString(t);
        } catch (JsonProcessingException e) {
            prettyJson = null;
        }

        System.out.println(prettyJson);
        return prettyJson;
    }

    @Override
    public String showConsumerGroup(String id) {
        ConsumerGroup cg = findConsumerGroup(id);
        String prettyJson;
        try {
            prettyJson = mapper.writeValueAsString(cg);
        } catch (JsonProcessingException e) {
            prettyJson = null;
        }

        System.out.println(prettyJson);
        return prettyJson;
    }

    @Override
    public String showProducer(String id) {
        Producer p = findProducer(id);

        String prettyJson;
        try {
            prettyJson = mapper.writeValueAsString(p);
        } catch (JsonProcessingException e) {
            prettyJson = null;
        }

        System.out.println(prettyJson);
        return prettyJson;
    }

    public void showAll() {
        System.out.println("Producers:");
        producerList.stream().forEach(p -> showProducer(p.getProducerId()));
        System.out.println("\nTopics:");
        topicList.stream().forEach(t -> showTopic(t.getTopicId()));
        System.out.println("\nConsumer Groups:");
        consumerGroupList.stream().forEach(cg -> showConsumerGroup(cg.getConsumerGroupId()));
    }

    @Override
    public Topic createTopic(String id, String type) {
        Topic topicAlreadyExist = findTopic(id);

        if (topicAlreadyExist == null) {
            Topic topic = new Topic(id, type);
            topicList.add(topic);
            System.out.println("Topic " + id + " of Type " + type + " created");
            return topic;
        } else {
            System.err.println("Topic " + id + " already exist");
            return null;
        }

    }

    @Override
    public Partition createPartition(String topicId, String partitionId) {
        Topic t = findTopic(topicId);
        if (t != null) {
            Partition partitionAlreadyExist = findPartition(partitionId);
            if (partitionAlreadyExist == null) {
                List<Partition> partitionList = t.getPartitionList();
                Partition p = new Partition(partitionId);
                partitionList.add(p);
                System.out.println("Partition " + partitionId + " added to Topic " + topicId);
                return p;
            } else {
                System.err.println("Partition " + partitionId + " already exist");
                return null;
            }

        } else {
            System.err.println("Topic doesn't exist");
            return null;
        }

    }

    @Override
    public ConsumerGroup createConsumerGroup(String consumerGroupId, String topicId, String balancingMethod) {
        if (validConsumerGroupBalancingMethods.contains(balancingMethod)) {
            ConsumerGroup consumerGroupAlreadyExist = findConsumerGroup(consumerGroupId);
            if (consumerGroupAlreadyExist == null) {
                ConsumerGroup cg = new ConsumerGroup(consumerGroupId, topicId, balancingMethod);
                consumerGroupList.add(cg);
                System.out.println("Consumer Group " + consumerGroupId + " created");
                return cg;
            } else {
                System.err.println("Consumer Group " + consumerGroupId + " already exist");
                return null;
            }
        } else {
            System.err.println("Invalid Balancing Method");
            return null;
        }

    }

    @Override
    public Consumer createConsumer(String consumerGroupId, String consumerId) {
        ConsumerGroup cg = findConsumerGroup(consumerGroupId);
        if (cg != null) {
            Consumer consumerAlreadyExist = findConsumer(consumerId);
            if (consumerAlreadyExist == null) {
                Consumer c = new Consumer(consumerGroupId, consumerId);
                cg.addConsumer(c);
                System.out.println("Consumer " + consumerId + " added to Consumer Group " + consumerGroupId);
                return c;
            } else {
                System.err.println("Consumer " + consumerId + " already exist");
                return null;
            }
        } else {
            System.err.println("Invalid Consumer Group");
            return null;
        }
    }

    @Override
    public Producer createProducer(String producerId, String type, String allocation) {
        Producer p;
        switch (allocation) {
        case "Random":
            p = new RandomProducer(producerId, type);
            break;
        case "Manual":
            p = new ManualProducer(producerId, type);
            break;
        default:
            System.err.println("Invalid Allocation Method");
            return null;
        }

        Producer producerAlreadyExist = findProducer(producerId);
        if (producerAlreadyExist == null) {
            producerList.add(p);
            System.out.println("Producer " + producerId + " created");
            return p;
        } else {
            System.err.println("Producer " + producerId + " already exist");
            return null;
        }
    }

    @Override
    public void deleteConsumer(String consumerId) {
        Consumer c = findConsumer(consumerId);
        if (c != null) {
            String consumerGroupId = c.getConsumerGroupId();
            ConsumerGroup cg = findConsumerGroup(consumerGroupId);
            cg.getConsumerList().remove(c);
            System.out.println("Consumer " + consumerId + " was deleted from Consumer Group " + consumerGroupId);
            System.out.println("Updated Consumer Group " + consumerGroupId);
            showConsumerGroup(consumerGroupId);
        } else {
            System.err.println("Invalid Consumer");
            return;
        }
    }

    @Override
    public Event produceEvent(String producerId, String topicId, String eventContent, String partitionId) {
        String eventID = UUID.randomUUID().toString();
        String headerID = UUID.randomUUID().toString();
        Header header = new Header(headerID);
        Event event = new Event(eventID, header, partitionId, eventContent);

        Producer producer = findProducer(producerId);
        Topic topic = findTopic(topicId);

        if (producer == null) {
            System.err.println("Invalid Producer");
            return null;
        }

        if (topic == null) {
            System.err.println("Invalid Topic");
            return null;
        }

        List<Partition> partitionList = topic.getPartitionList();

        Boolean assigned = producer.assignEvent(event, partitionList, partitionId);
        if (!assigned) {
            System.err.println("Invalid Assignment");
            return null;
        }
        System.out.println("Event " + eventID + " is part of Partition " + partitionId);
        return event;
    }

    @Override
    public Event consumeEvent(String consumerId, String partitionId) {
        Consumer c = findConsumer(consumerId);
        Partition p = findPartition(partitionId);

        if (c == null) {
            System.err.println("Invalid Consumer");
            return null;
        }

        if (p == null) {
            System.err.println("Invalid Partition");
            return null;
        }

        Event event = c.consume(p);
        return event;
    }

    @Override
    public List<Event> consumeEvents(String consumerId, String partitionId, int numberOfEvents) {
        Consumer c = findConsumer(consumerId);
        for (int i = 0; i < numberOfEvents; i++) {
            consumeEvent(consumerId, partitionId);
        }
        List<Event> consumedEvents = c.getConsumedEventList();
        return consumedEvents;
    }

    @Override
    public ConsumerGroup setConsumerGroupRebalancing(String consumerGroupId, String balancingMethod) {
        ConsumerGroup cg = findConsumerGroup(consumerGroupId);

        if (cg == null) {
            System.err.println("Invalid Consumer Group");
            return null;

        }

        if (!validConsumerGroupBalancingMethods.contains(balancingMethod)) {
            System.err.println("Invalid Balancing Method Group");
            return null;
        }

        cg.setBalancingMethod(balancingMethod);
        System.out.println("Consumer Group " + consumerGroupId + " now use " + balancingMethod);
        return cg;

    }
}
