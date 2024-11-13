package tributary.core;

public class Message {
    private Header header;
    private String key;
    private String value;

    public Message(Header header) {
        this.header = header;
    }

    public Header getHeader() {
        return header;
    }
}
