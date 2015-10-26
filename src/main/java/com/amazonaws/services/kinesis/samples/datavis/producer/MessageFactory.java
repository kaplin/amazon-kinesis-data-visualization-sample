package com.amazonaws.services.kinesis.samples.datavis.producer;

import com.amazonaws.services.kinesis.samples.datavis.model.HealthCheckStateMessage;

import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.UUID;

public class MessageFactory {

    private Random random = new Random();

    public HealthCheckStateMessage create() {
        HealthCheckStateMessage message = new HealthCheckStateMessage();
        HashMap<UUID, Byte> state = new HashMap<>();
        for (int i = 0; i < 5440; i++) {
            state.put(UUID.randomUUID(), (byte) random.nextInt(255));
        }
        message.setState(state);
        message.setTimestamp(new Date());
        message.setStartingSequenceNumber(random.nextInt());
        return message;
    }
}
