package tributary.core;

public class Event {
    private String eventId;
    private Header header;
    private String key;
    private String value;

    public Event(String eventId, Header header, String key, String value) {
        this.eventId = eventId;
        this.header = header;
        this.key = key;
        this.value = value;
    }

    public Header getHeader() {
        return header;
    }
}
