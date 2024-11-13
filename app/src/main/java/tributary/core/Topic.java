package tributary.core;

import java.util.List;
import java.util.ArrayList;

public class Topic {
    private String id;
    private String type;
    private List<Partition> partitionList = new ArrayList<>();

    public Topic(String id, String type) {
        this.id = id;
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public List<Partition> getPartitionList() {
        return partitionList;
    }

}
