/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m_ld.guicicle.channel.Channel;
import org.m_ld.guicicle.channel.ChannelOptions;

import java.io.IOException;

import static io.vertx.core.MultiMap.caseInsensitiveMultiMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class VertxMqttPublishTest extends VertxMqttWithBrokerTest
{
    @BeforeClass public static void setUp(TestContext context) throws IOException
    {
        VertxMqttWithBrokerTest.setUp(context, new JsonObject()); // All default options
    }

    @Test public void testPublishString(TestContext context)
    {
        final Async done = context.async();
        TestModule.run(channels -> {
            channels.channel("testPublishString",
                             new ChannelOptions().addHeader("test", "fred")).producer().write("Hello");
            published.get().setHandler(context.asyncAssertSuccess(msg -> {
                MultiMap headers = caseInsensitiveMultiMap();
                assertEquals("Hello", EVENT_CODEC.decodeFromWire(msg.payload(), headers));
                assertEquals("fred", headers.get("test"));
                assertEquals(MqttQoS.AT_MOST_ONCE, msg.qosLevel());
                assertEquals("testPublishString", msg.topicName());
                done.complete();
            }));
        });
    }

    @Test public void testNoSendBecauseNoPresence(TestContext context)
    {
        final Async done = context.async();
        TestModule.run(channels -> {
            try
            {
                channels.channel("testNoSendBecauseNoPresence",
                                 new ChannelOptions()).producer().send("Hello");
                context.fail();
            }
            catch (UnsupportedOperationException e)
            {
                done.complete();
            }
        });
    }

    @Test public void testEchoString(TestContext context)
    {
        final Async done = context.async();
        TestModule.run(channels -> {
            final Channel<String> channel = channels.channel("testEchoString",
                                                             new ChannelOptions().setEcho(true));
            channel.consumer()
                .handler(msg -> {
                    assertEquals("Hello", msg.body());
                    assertEquals("testEchoString", msg.address());
                    assertFalse(msg.isSend());
                    done.complete();
                })
                // Write the message when the consumer has been registered
                .completionHandler(context.asyncAssertSuccess(v -> channel.producer().write("Hello")));
        });
    }

    @Test public void testNoEchoInteger(TestContext context)
    {
        final Async done = context.async();
        TestModule.run(channels -> {
            final Channel<Integer> channel = channels.channel("testNoEchoInteger",
                                                              new ChannelOptions().setEcho(false));
            channel.consumer()
                .handler(msg -> {
                    if (msg.body() == 2)
                        done.complete();
                    else
                        context.fail();
                })
                .completionHandler(context.asyncAssertSuccess(v -> {
                    final MessageProducer<Integer> producer = channel.producer();
                    producer.write(1);
                    producer.deliveryOptions(new ChannelOptions().setEcho(true));
                    producer.write(2);
                }));
        });
    }
}
