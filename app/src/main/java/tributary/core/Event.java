package tributary.core;

public class Event<T> {
    private String eventId;
    private Header<T> header;
    private String key;
    private T value;

    public Event(String eventId, Header<T> header, String key, T value) {
        this.eventId = eventId;
        this.header = header;
        this.key = key;
        this.value = value;
    }

    public Header<T> getHeader() {
        return header;
    }

    public String getEventId() {
        return eventId;
    }

    public String getKey() {
        return key;
    }

    public T getValue() {
        return value;
    }

}
