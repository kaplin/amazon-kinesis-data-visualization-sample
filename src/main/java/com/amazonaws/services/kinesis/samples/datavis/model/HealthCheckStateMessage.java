package com.amazonaws.services.kinesis.samples.datavis.model;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class HealthCheckStateMessage {
    private int startingSequenceNumber;
    private Date timestamp;
    private Map<UUID, Byte> state;

    public int getStartingSequenceNumber() {
        return startingSequenceNumber;
    }

    public void setStartingSequenceNumber(int startingSequenceNumber) {
        this.startingSequenceNumber = startingSequenceNumber;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Map<UUID, Byte> getState() {
        return state;
    }

    public void setState(Map<UUID, Byte> state) {
        this.state = state;
    }
}
