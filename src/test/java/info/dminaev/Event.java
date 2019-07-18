package info.dminaev;

import java.io.Serializable;

public class Event
        implements Serializable
{
    private long timestamp;

    private String payload;

    public Event( long timestamp, String payload )
    {
        this.timestamp = timestamp;
        this.payload = payload;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp( long timestamp )
    {
        this.timestamp = timestamp;
    }

    public String getPayload()
    {
        return payload;
    }

    public void setPayload( String payload )
    {
        this.payload = payload;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }

        Event event = (Event) o;

        if ( timestamp != event.timestamp ) {
            return false;
        }
        return payload != null ? payload.equals(event.payload) : event.payload == null;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (payload != null ? payload.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "Event{" + "timestamp=" + timestamp + ", payload='" + payload + '\'' + '}';
    }
}
