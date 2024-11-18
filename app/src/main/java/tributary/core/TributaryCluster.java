package tributary.core;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.json.JSONObject;
import org.json.JSONTokener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import tributary.api.TributaryService;

public class TributaryCluster<T> implements TributaryService<T> {
    private List<Producer<T>> producerList = new ArrayList<>();
    private List<Topic<T>> topicList = new ArrayList<>();
    private List<ConsumerGroup<T>> consumerGroupList = new ArrayList<>();

    private List<String> validConsumerGroupBalancingMethods = new ArrayList<>();

    private ObjectMapper mapper; // Pretty Printer

    public TributaryCluster() {
        mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        validConsumerGroupBalancingMethods.add("Range");
        validConsumerGroupBalancingMethods.add("RoundRobin");
    }

    private void setProducerList(List<Producer<T>> producerList) {
        this.producerList = producerList;
    }

    private void setTopicList(List<Topic<T>> topicList) {
        this.topicList = topicList;
    }

    private void setConsumerGroupList(List<ConsumerGroup<T>> consumerGroupList) {
        this.consumerGroupList = consumerGroupList;
    }

    public void clearTributaryCluster() {
        setProducerList(null);
        setTopicList(null);
        setConsumerGroupList(null);
    }

    private Topic<T> findTopic(String id) {
        return topicList.stream().filter(topic -> topic.getTopicId().equals(id)).findAny().orElse(null);
    }

    private Partition<T> findPartition(String id) {
        Partition<T> p;
        for (Topic<T> t : topicList) {
            p = t.getPartitionList().stream().filter(partition -> partition.getPartitionId().equals(id)).findAny()
                    .orElse(null);
            if (p != null) {
                return p;
            }
        }
        return null;
    }

    private Producer<T> findProducer(String id) {
        return producerList.stream().filter(producer -> producer.getProducerId().equals(id)).findAny().orElse(null);
    }

    private ConsumerGroup<T> findConsumerGroup(String id) {
        return consumerGroupList.stream().filter(group -> group.getConsumerGroupId().equals(id)).findAny().orElse(null);
    }

    private Consumer<T> findConsumer(String id) {
        Consumer<T> c;
        for (ConsumerGroup<T> cg : consumerGroupList) {
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
        Topic<T> t = findTopic(id);
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
        ConsumerGroup<T> cg = findConsumerGroup(id);
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
        Producer<T> p = findProducer(id);

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
    public Topic<T> createTopic(String id, String type) {
        Topic<T> topicAlreadyExist = findTopic(id);

        if (topicAlreadyExist == null) {
            Topic<T> topic = new Topic<T>(id, type);
            topicList.add(topic);
            System.out.println("Topic " + id + " of Type " + type + " created");
            return topic;
        } else {
            System.err.println("Topic " + id + " already exist");
            return null;
        }

    }

    @Override
    public Partition<T> createPartition(String topicId, String partitionId) {
        Topic<T> t = findTopic(topicId);
        if (t != null) {
            Partition<T> partitionAlreadyExist = findPartition(partitionId);
            if (partitionAlreadyExist == null) {
                List<Partition<T>> partitionList = t.getPartitionList();
                Partition<T> p = new Partition<T>(partitionId);
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
    public ConsumerGroup<T> createConsumerGroup(String consumerGroupId, String topicId, String balancingMethod) {
        if (validConsumerGroupBalancingMethods.contains(balancingMethod)) {
            ConsumerGroup<T> consumerGroupAlreadyExist = findConsumerGroup(consumerGroupId);
            if (consumerGroupAlreadyExist == null) {
                ConsumerGroup<T> cg = new ConsumerGroup<T>(consumerGroupId, topicId, balancingMethod);
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
    public Consumer<T> createConsumer(String consumerGroupId, String consumerId) {
        ConsumerGroup<T> cg = findConsumerGroup(consumerGroupId);
        if (cg != null) {
            Consumer<T> consumerAlreadyExist = findConsumer(consumerId);
            if (consumerAlreadyExist == null) {
                Consumer<T> c = new Consumer<T>(consumerGroupId, consumerId);
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
    public Producer<T> createProducer(String producerId, String type, String allocation) {
        Producer<T> p;
        switch (allocation) {
        case "Random":
            p = new RandomProducer<T>(producerId, type);
            break;
        case "Manual":
            p = new ManualProducer<T>(producerId, type);
            break;
        default:
            System.err.println("Invalid Allocation Method");
            return null;
        }

        Producer<T> producerAlreadyExist = findProducer(producerId);
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
        Consumer<T> c = findConsumer(consumerId);
        if (c != null) {
            String consumerGroupId = c.getConsumerGroupId();
            ConsumerGroup<T> cg = findConsumerGroup(consumerGroupId);
            cg.getConsumerList().remove(c);
            System.out.println("Consumer " + consumerId + " was deleted from Consumer Group " + consumerGroupId);
            System.out.println("Updated Consumer Group " + consumerGroupId);
            showConsumerGroup(consumerGroupId);
        } else {
            System.err.println("Invalid Consumer");
            return;
        }
    }

    // @Override
    public Event<T> produceEvent(String producerId, String topicId, String eventContentFilePath, String partitionId,
            Class<T> valueType) {
        String eventID = UUID.randomUUID().toString();
        String headerID = UUID.randomUUID().toString();
        Header<T> header = new Header<T>(headerID, valueType);

        T value = parseJsonToEvent(eventContentFilePath, valueType);

        Event<T> event = new Event<T>(eventID, header, partitionId, value);

        Producer<T> producer = findProducer(producerId);
        Topic<T> topic = findTopic(topicId);

        if (producer == null) {
            System.err.println("Invalid Producer");
            return null;
        }

        if (topic == null) {
            System.err.println("Invalid Topic");
            return null;
        }

        List<Partition<T>> partitionList = topic.getPartitionList();

        Boolean assigned = producer.assignEvent(event, partitionList, partitionId);
        if (!assigned) {
            System.err.println("Invalid Assignment");
            return null;
        }
        System.out.println("Event " + eventID + " is part of Partition " + partitionId);
        return event;
    }

    @Override
    public Event<T> consumeEvent(String consumerId, String partitionId) {
        Consumer<T> c = findConsumer(consumerId);
        Partition<T> p = findPartition(partitionId);

        if (c == null) {
            System.err.println("Invalid Consumer");
            return null;
        }

        if (p == null) {
            System.err.println("Invalid Partition");
            return null;
        }

        Event<T> event = c.consume(p);
        return event;
    }

    @Override
    public List<Event<T>> consumeEvents(String consumerId, String partitionId, int numberOfEvents) {
        Consumer<T> c = findConsumer(consumerId);
        for (int i = 0; i < numberOfEvents; i++) {
            consumeEvent(consumerId, partitionId);
        }
        List<Event<T>> consumedEvents = c.getConsumedEventList();
        return consumedEvents;
    }

    @Override
    public ConsumerGroup<T> setConsumerGroupRebalancing(String consumerGroupId, String balancingMethod) {
        ConsumerGroup<T> cg = findConsumerGroup(consumerGroupId);

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

    // helper method to parse JSON to event using JSON object:
    public T parseJsonToEvent(String eventFilePath, Class<T> classType) {
        try {
            // InputStream inputStream = new FileInputStream(eventFilePath);
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(eventFilePath);
            InputStreamReader streamReader = new InputStreamReader(inputStream);
            JSONObject jsonObject = new JSONObject(new JSONTokener(streamReader));
            if (classType == String.class) {
                return classType.cast(jsonObject.getString("value"));
            } else if (classType == Integer.class) {
                return classType.cast(jsonObject.getInt("value"));
            }
        } catch (Exception exception) {
            System.out.println("Error reading or parsing JSON from file: " + eventFilePath);
            exception.printStackTrace();
        }
        return null;
    }

    public void parallelProduce(List<Producer<T>> producers, String topicId, List<Event<T>> events) {
        Topic<T> topic = findTopic(topicId);
        Thread[] threads = new Thread[producers.size()];

        for (int i = 0; i < producers.size(); i++) {
            final Producer<T> producer = producers.get(i);
            final Event<T> event = events.get(i);

            threads[i] = new Thread(() -> {
                synchronized (topic) {
                    List<Partition<T>> partitions = topic.getPartitionList();
                    producer.assignEvent(event, partitions, null); // Assign to a random partition
                    System.out.println("Produced event: " + event.getEventId());
                }
            });

            threads[i].start();
        }

        for (Thread thread : threads) {
            try {
                thread.join(); // Wait for all threads to finish
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void parallelConsume(List<Consumer<T>> consumers, List<Partition<T>> partitions) {
        Thread[] threads = new Thread[consumers.size()];

        for (int i = 0; i < consumers.size(); i++) {
            final Consumer<T> consumer = consumers.get(i);
            final Partition<T> partition = partitions.get(i % partitions.size());

            threads[i] = new Thread(() -> {
                synchronized (partition) {
                    Event<T> event = consumer.consume(partition);
                    if (event != null) {
                        System.out.println("Consumed event: " + event.getEventId() + ", Content: " + event.getValue());
                    } else {
                        System.out.println("No events left to consume in partition: " + partition.getPartitionId());
                    }
                }
            });

            threads[i].start();
        }

        for (Thread thread : threads) {
            try {
                thread.join(); // Wait for all threads to finish
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
