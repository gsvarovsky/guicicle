/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import io.vertx.core.AsyncResult;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.messages.MqttPublishMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.atomic.AtomicReference;

import static io.vertx.core.Future.succeededFuture;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.*;
import static org.m_ld.guicicle.channel.ChannelOptions.Quality.AT_LEAST_ONCE;
import static org.m_ld.guicicle.channel.ChannelOptions.Quality.AT_MOST_ONCE;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class) public class MqttPresenceTest
{
    @Mock MqttEventClient mqtt;
    MqttPresence presence;

    @Before public void setUp()
    {
        when(mqtt.clientId()).thenReturn("client1");
        presence = new MqttPresence(mqtt, "domain");
        presence.onSubscribe(succeededFuture());
    }

    @Test
    public void testShutdownUnsubscribes()
    {
        final AtomicReference<AsyncResult<Void>> closed = new AtomicReference<>();
        presence.close(closed::set);
        assertNull(closed.get());
        verify(mqtt).unsubscribe(presence);
        presence.onUnsubscribe(succeededFuture());
        assertTrue(closed.get().succeeded());
    }

    @Test public void testJoin()
    {
        final AtomicReference<AsyncResult<Void>> joined = new AtomicReference<>();
        when(mqtt.publish("__presence/domain/client1/channel1",
                          Buffer.buffer("address"),
                          AT_LEAST_ONCE,
                          false,
                          true)).thenReturn(succeededFuture(1));
        presence.join("channel1", "address", joined::set);
        presence.onPublished(1);
        assertTrue(joined.get().succeeded());
    }

    @Test public void testJoinBeforeSubscribe()
    {
        presence = new MqttPresence(mqtt, "domain");
        final AtomicReference<AsyncResult<Void>> joined = new AtomicReference<>();
        presence.join("channel1", "address", joined::set);
        assertNull(joined.get());

        when(mqtt.publish("__presence/domain/client1/channel1",
                          Buffer.buffer("address"),
                          AT_LEAST_ONCE,
                          false,
                          true)).thenReturn(succeededFuture(1));
        presence.onSubscribe(succeededFuture());
        presence.onPublished(1);
        assertTrue(joined.get().succeeded());
    }

    @Test public void testLeave()
    {
        final AtomicReference<AsyncResult<Void>> left = new AtomicReference<>();
        when(mqtt.publish("__presence/domain/client1/channel1",
                          Buffer.buffer("-"),
                          AT_MOST_ONCE,
                          false,
                          false)).thenReturn(succeededFuture());
        presence.leave("channel1", left::set);
        assertTrue(left.get().succeeded());
    }

    @Test(expected = IllegalArgumentException.class) public void testNoConsumeRandomMessage()
    {
        final MqttPublishMessage msg = mock(MqttPublishMessage.class);
        when(msg.topicName()).thenReturn("random/stuff");
        presence.onMessage(msg, presence.subscriptions()[0]);
    }

    @Test public void testConsumeConsumerConnectedMessage()
    {
        final MqttPublishMessage msg = mock(MqttPublishMessage.class);
        when(msg.topicName()).thenReturn("__presence/domain/client1/channel1");
        when(msg.payload()).thenReturn(Buffer.buffer("address"));
        presence.onMessage(msg, presence.subscriptions()[0]);
        assertEquals(singleton("channel1"), presence.present("address"));
    }

    @Test public void testPresenceMatchesAddress()
    {
        final MqttPublishMessage msg = mock(MqttPublishMessage.class);
        when(msg.topicName()).thenReturn("__presence/domain/client1/channel1");
        when(msg.payload()).thenReturn(Buffer.buffer("#"));
        presence.onMessage(msg, presence.subscriptions()[0]);
        assertEquals(singleton("channel1"), presence.present("address"));
    }

    @Test public void testConsumeUnknownConsumerDisconnectedMessage()
    {
        final MqttPublishMessage msg = mock(MqttPublishMessage.class);
        when(msg.topicName()).thenReturn("__presence/domain/client1/channel1");
        when(msg.payload()).thenReturn(Buffer.buffer("-"));
        presence.onMessage(msg, presence.subscriptions()[0]);
        assertEquals(emptySet(), presence.present("address"));
    }

    @Test public void testConsumeKnownConsumerDisconnectedMessage()
    {
        final MqttPublishMessage msg1 = mock(MqttPublishMessage.class), msg2 = mock(MqttPublishMessage.class);
        when(msg1.topicName()).thenReturn("__presence/domain/client1/channel1");
        when(msg1.payload()).thenReturn(Buffer.buffer("address"));
        when(msg2.topicName()).thenReturn("__presence/domain/client1/channel1");
        when(msg2.payload()).thenReturn(Buffer.buffer("-"));
        presence.onMessage(msg1, presence.subscriptions()[0]);
        presence.onMessage(msg2, presence.subscriptions()[0]);
        assertEquals(emptySet(), presence.present("address"));
    }

    @Test public void testConsumeClientLwtMessage()
    {
        final MqttPublishMessage msg1 = mock(MqttPublishMessage.class), msg2 = mock(MqttPublishMessage.class);
        when(msg1.topicName()).thenReturn("__presence/domain/client1/channel1");
        when(msg1.payload()).thenReturn(Buffer.buffer("address"));
        when(msg2.topicName()).thenReturn("__presence/domain/client1");
        when(msg2.payload()).thenReturn(Buffer.buffer("-"));
        presence.onMessage(msg1, presence.subscriptions()[0]);
        presence.onMessage(msg2, presence.subscriptions()[0]);
        assertEquals(emptySet(), presence.present("address"));
    }
}