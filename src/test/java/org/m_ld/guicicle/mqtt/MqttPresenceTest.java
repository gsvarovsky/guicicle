/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.messages.MqttPublishMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class) public class MqttPresenceTest
{
    @Mock MqttClient mqtt;

    @Before
    public void setUp()
    {
        when(mqtt.clientId()).thenReturn("client1");
    }

    @Test public void testJoin()
    {
        new MqttPresence(mqtt).join("channel1", "address");
        verify(mqtt).publish("__presence/client1/channel1",
                             Buffer.buffer("address"),
                             MqttQoS.AT_MOST_ONCE,
                             false,
                             true);
    }

    @Test public void testLeave()
    {
        new MqttPresence(mqtt).leave("channel1");
        verify(mqtt).publish("__presence/client1/channel1",
                             Buffer.buffer("-"),
                             MqttQoS.AT_MOST_ONCE,
                             false,
                             true);
    }

    @Test public void testNoConsumeRandomMessage()
    {
        final MqttPublishMessage msg = mock(MqttPublishMessage.class);
        when(msg.topicName()).thenReturn("random/stuff");
        final MqttPresence presence = new MqttPresence(mqtt);
        presence.onMessage(msg);
    }

    @Test public void testConsumeConsumerConnectedMessage()
    {
        final MqttPublishMessage msg = mock(MqttPublishMessage.class);
        when(msg.topicName()).thenReturn("__presence/client1/channel1");
        when(msg.payload()).thenReturn(Buffer.buffer("address"));
        final MqttPresence presence = new MqttPresence(mqtt);
        presence.onMessage(msg);
        assertEquals(singleton("channel1"), presence.present("address"));
    }

    @Test public void testPresenceMatchesAddress()
    {
        final MqttPublishMessage msg = mock(MqttPublishMessage.class);
        when(msg.topicName()).thenReturn("__presence/client1/channel1");
        when(msg.payload()).thenReturn(Buffer.buffer("#"));
        final MqttPresence presence = new MqttPresence(mqtt);
        presence.onMessage(msg);
        assertEquals(singleton("channel1"), presence.present("address"));
    }

    @Test public void testConsumeUnknownConsumerDisconnectedMessage()
    {
        final MqttPublishMessage msg = mock(MqttPublishMessage.class);
        when(msg.topicName()).thenReturn("__presence/client1/channel1");
        when(msg.payload()).thenReturn(Buffer.buffer("-"));
        final MqttPresence presence = new MqttPresence(mqtt);
        presence.onMessage(msg);
        assertEquals(emptySet(), presence.present("address"));
    }

    @Test public void testConsumeKnownConsumerDisconnectedMessage()
    {
        final MqttPublishMessage msg1 = mock(MqttPublishMessage.class), msg2 = mock(MqttPublishMessage.class);
        when(msg1.topicName()).thenReturn("__presence/client1/channel1");
        when(msg1.payload()).thenReturn(Buffer.buffer("address"));
        when(msg2.topicName()).thenReturn("__presence/client1/channel1");
        when(msg2.payload()).thenReturn(Buffer.buffer("-"));
        final MqttPresence presence = new MqttPresence(mqtt);
        presence.onMessage(msg1);
        presence.onMessage(msg2);
        assertEquals(emptySet(), presence.present("address"));
    }

    @Test public void testConsumeClientLwtMessage()
    {
        final MqttPublishMessage msg1 = mock(MqttPublishMessage.class), msg2 = mock(MqttPublishMessage.class);
        when(msg1.topicName()).thenReturn("__presence/client1/channel1");
        when(msg1.payload()).thenReturn(Buffer.buffer("address"));
        when(msg2.topicName()).thenReturn("__presence/client1");
        when(msg2.payload()).thenReturn(Buffer.buffer("-"));
        final MqttPresence presence = new MqttPresence(mqtt);
        presence.onMessage(msg1);
        presence.onMessage(msg2);
        assertEquals(emptySet(), presence.present("address"));
    }
}