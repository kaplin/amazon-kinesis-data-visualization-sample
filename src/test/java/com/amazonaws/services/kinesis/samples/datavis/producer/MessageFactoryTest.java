package com.amazonaws.services.kinesis.samples.datavis.producer;

import com.amazonaws.services.kinesis.samples.datavis.model.HealthCheckStateMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class MessageFactoryTest {

    private final ObjectMapper JSON = new ObjectMapper();

    @Test
    public void testCreate() throws Exception {
        HealthCheckStateMessage message = new MessageFactory().create();
        assertEquals(5440, message.getState().size());
        assertNotNull(message.getTimestamp());
        assertNotEquals(0, message.getStartingSequenceNumber());
    }
}