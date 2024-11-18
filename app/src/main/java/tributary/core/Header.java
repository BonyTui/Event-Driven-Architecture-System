package tributary.core;

public class Header<T> {
    private String headerId;
    private long dateTimeCreated;
    private String payloadType;

    public Header(String headerId, Class<T> valueType) {
        dateTimeCreated = System.currentTimeMillis();
        this.headerId = headerId;
        this.payloadType = valueType.getSimpleName();
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

    public void setPayloadType(String payloadType) {
        this.payloadType = payloadType;
    }
}
