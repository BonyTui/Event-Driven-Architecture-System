package tributary.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import tributary.api.TributaryService;

public class TributaryCluster implements TributaryService {
    private List<Topic> topicList = new ArrayList<>();

    private Topic findTopic(String id) {
        return topicList.stream().filter(topic -> topic.getId().equals(id)).findAny().orElse(null);
    }

    public void createTopic(String id, String type) {
        Topic topic = new Topic(id, type);
        topicList.add(topic);
        System.out.println("Topic " + id + " of Type " + type + " created");
    }

    public void showTopic(String id) {
        Topic t = findTopic(id);

        if (t != null) {
            System.out.println("Topic: " + t.getId());

            List<Partition> partitionList = t.getPartitionList();
            if (partitionList.isEmpty()) {
                System.out.println("No partitions");
            } else {
                System.out.println("Partitions:");
                for (Partition p : partitionList) {
                    System.out.println(p.getId());

                    Queue<Message> messageQueue = p.getMessageQueue();
                    System.out.println("Messages:");
                    if (messageQueue.isEmpty()) {
                        System.out.println("No messages");
                    } else {
                        for (Message m : messageQueue) {
                            System.out.println(m.getHeader().getId());
                        }
                    }
                }
            }
        } else {
            System.out.println("Topic doesn't exist");
        }
    }

    public void createPartition(String topicId, String partitionId) {
        Topic t = findTopic(topicId);
        if (t != null) {
            List<Partition> partitionList = t.getPartitionList();
            Partition p = new Partition(partitionId);
            partitionList.add(p);
            System.out.println("Partition " + partitionId + " added to Topic " + topicId);
        } else {
            System.out.println("Topic doesn't exist");
        }
    }
}
