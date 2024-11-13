package tributary.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import tributary.api.TributaryService;

public class TributaryCluster implements TributaryService {
    private List<Producer> producerList = new ArrayList<>();
    private List<Topic> topicList = new ArrayList<>();
    private List<ConsumerGroup> consumerGroupList = new ArrayList<>();

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

    private Producer findProducer(String id) {
        return producerList.stream().filter(producer -> producer.getProducerId().equals(id)).findAny().orElse(null);
    }

    private ConsumerGroup findConsumerGroup(String id) {
        return consumerGroupList.stream().filter(group -> group.getConsumerGroupId().equals(id)).findAny().orElse(null);
    }

    public String showTopic(String id) {
        String returnString = "";
        Topic t = findTopic(id);

        if (t != null) {
            returnString += ("Topic: " + t.getTopicId() + "\n");

            List<Partition> partitionList = t.getPartitionList();
            if (partitionList.isEmpty()) {
                returnString += ("No partitions\n");
            } else {
                returnString += ("Partitions:\n");
                for (Partition p : partitionList) {
                    System.out.println(p.getId());

                    Queue<Message> messageQueue = p.getMessageQueue();
                    returnString += ("Messages:\n");
                    if (messageQueue.isEmpty()) {
                        returnString += ("No messages\n");
                    } else {
                        for (Message m : messageQueue) {
                            System.out.println(m.getHeader().getId());
                        }
                    }
                }
            }
            return returnString;
        } else {
            returnString += ("Topic doesn't exist\n");
            return returnString;
        }
    }

    public String showConsumerGroup(String id) {
        ConsumerGroup cg = findConsumerGroup(id);

        if (cg != null) {
            System.out.println("Consumer Group: " + cg.getConsumerGroupId());

            List<Consumer> consumerList = cg.getConsumerList();
            if (consumerList.isEmpty()) {
                System.out.println("No consumer");
            } else {
                for (Consumer c : consumerList) {
                    System.out.println(
                            "Consumer " + c.getConsumerId() + " is receiving events from " + c.getPartitionId());
                }
            }
        } else {
            System.err.println("Consumer Group doesn't exist");
            return;
        }
    }

    private String showProducer(String id) {
        Producer p = findProducer(id);

        if (p != null) {
            System.out.println("Producer: " + p.getProducerId());
        } else {
            System.err.println("Producer doesn't exist");
            return;
        }
    }

    public void showAll() {
        System.out.println("Producers:");
        producerList.stream().forEach(p -> showProducer(p.getProducerId()));
        System.out.println("\nTopics:");
        topicList.stream().forEach(t -> showTopic(t.getTopicId()));
        System.out.println("\nConsumer Groups:");
        consumerGroupList.stream().forEach(cg -> showConsumerGroup(cg.getConsumerGroupId()));
    }

    public void createTopic(String id, String type) {
        Topic topic = new Topic(id, type);
        topicList.add(topic);
        System.out.println("Topic " + id + " of Type " + type + " created");
    }

    public void createPartition(String topicId, String partitionId) {
        Topic t = findTopic(topicId);
        if (t != null) {
            List<Partition> partitionList = t.getPartitionList();
            Partition p = new Partition(partitionId);
            partitionList.add(p);
            System.out.println("Partition " + partitionId + " added to Topic " + topicId);
        } else {
            System.err.println("Topic doesn't exist");
            return;
        }
    }

    public void createConsumerGroup(String consumerGroupId, String topicId, String balancingMethod) {
        ConsumerGroup cg;
        if (balancingMethod.equals("Range") || balancingMethod.equals("RoundRobin")) {
            cg = new ConsumerGroup(consumerGroupId, topicId, balancingMethod);
        } else {
            System.err.println("Invalid Balancing Method");
            return;
        }

        Topic t = findTopic(topicId);
        if (t != null) {
            consumerGroupList.add(cg);
            System.out.println("Consumer Group " + consumerGroupId + " created");
        } else {
            System.err.println("Invalid Topic");
            return;
        }
    }

    public void createConsumer(String consumerGroupId, String consumerId) {
        ConsumerGroup cg = findConsumerGroup(consumerGroupId);
        if (cg != null) {
            Consumer c = new Consumer(consumerGroupId, consumerId);
            cg.addConsumer(c);
            System.out.println("Consumer " + consumerId + " added to Consumer Group " + consumerGroupId);
        } else {
            System.err.println("Invalid Consumer Group");
            return;
        }
    }

    public void createProducer(String producerId, String type, String allocation) {
        Producer p;
        switch (allocation) {
        case "Random":
            p = new RandomProducer(producerId, type);
            break;
        case "Manual":
            p = new ManualProducer(producerId, type);
            break;
        default:
            System.out.println("Invalid Allocation Method");
            return;
        }

        producerList.add(p);
        System.out.println("Producer " + producerId + " created");
    }
}
