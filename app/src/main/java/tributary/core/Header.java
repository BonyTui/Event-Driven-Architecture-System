package tributary.core;

public class Header {
    private String headerId;
    private long dateTimeCreated;
    private String payloadType;

    public Header(String headerId) {
        dateTimeCreated = System.currentTimeMillis();
        this.headerId = headerId;
        this.payloadType = "String";
    }

    public String getHeaderId() {
        return headerId;
    }

    public long getDateTimeCreated() {
        return dateTimeCreated;
    }

    public String getPayloadType() {
        return payloadType;
    }
}
