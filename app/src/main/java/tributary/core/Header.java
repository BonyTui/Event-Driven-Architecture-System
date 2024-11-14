package tributary.core;

import java.time.LocalDateTime;

public class Header {
    private String headerId;
    private LocalDateTime dateTimeCreated;
    private String payloadType;

    public Header(String headerId) {
        dateTimeCreated = java.time.LocalDateTime.now();
        this.headerId = headerId;
        this.payloadType = "String";
    }

    public String getHeaderId() {
        return headerId;
    }

    public LocalDateTime getDateTimeCreated() {
        return dateTimeCreated;
    }

    public String getPayloadType() {
        return payloadType;
    }
}
