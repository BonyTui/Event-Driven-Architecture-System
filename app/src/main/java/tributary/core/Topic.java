package tributary.core;

import java.util.List;
import java.util.ArrayList;

public class Topic<T> {
    private String topicId;
    private String topicType;
    private List<Partition<T>> partitionList = new ArrayList<>();

    public Topic(String topicId, String topicType) {
        this.topicId = topicId;
        this.topicType = topicType;
    }

    public String getTopicType() {
        return topicType;
    }

    public String getTopicId() {
        return topicId;
    }

    public String getType() {
        return topicType;
    }

    public List<Partition<T>> getPartitionList() {
        return partitionList;
    }

}
