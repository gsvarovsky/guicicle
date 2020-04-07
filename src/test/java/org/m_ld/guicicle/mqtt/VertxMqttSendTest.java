/*
 * Copyright (c) George Svarovsky 2020. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m_ld.guicicle.channel.Channel;
import org.m_ld.guicicle.channel.ChannelOptions;

import java.io.IOException;

import static org.junit.Assert.*;

public class VertxMqttSendTest extends VertxMqttWithBrokerTest
{
    @BeforeClass public static void setUp(TestContext context) throws IOException
    {
        VertxMqttWithBrokerTest.setUp(context, new JsonObject().put("presence.domain", "test"));
    }

    private static ChannelOptions sendOptions()
    {
        return new ChannelOptions().setPresence(true).setSendTimeout(1000);
    }

    @Test public void cannotSetSendWithoutPresence(TestContext context)
    {
        final Async done = context.async();
        TestModule.run(channels -> {
            try
            {
                channels.channel("cannotSetSendWithoutPresence",
                                 new ChannelOptions().setDelivery(ChannelOptions.Delivery.SEND));
                fail();
            }
            catch (UnsupportedOperationException ignored)
            {
                done.complete();
            }
        });
    }

    @Test public void testSendStringToSelf(TestContext context)
    {
        final Async done = context.async();
        TestModule.run(channels -> {
            final Channel<String> channel = channels.channel("testSendStringToSelf",
                                                             sendOptions().setEcho(true));
            channel.consumer()
                .handler(msg -> {
                    assertEquals("Hello", msg.body());
                    assertEquals("testSendStringToSelf", msg.address());
                    assertTrue(msg.isSend());
                    done.complete();
                })
                // Write the message when the consumer has been registered
                .completionHandler(context.asyncAssertSuccess(v -> channel.producer().send("Hello")));
        });
    }

    @Test public void testSendStringToNonSelf(TestContext context)
    {
        final Async done = context.async();
        TestModule.run(channels -> {
            final Channel<String>
                channel1 = channels.channel("testSendStringToNonSelf", sendOptions().setEcho(false)),
                channel2 = channels.channel("testSendStringToNonSelf", sendOptions());
            final Future<Void> channel1Connected = Future.future(), channel2Connected = Future.future();

            channel1.consumer()
                // Fail if the message echoes
                .handler(msg -> context.fail())
                .completionHandler(channel1Connected);

            channel2.consumer()
                .handler(msg -> {
                    assertEquals("Hello", msg.body());
                    done.complete();
                })
                .completionHandler(channel2Connected);

            CompositeFuture.all(channel1Connected, channel2Connected)
                .setHandler(context.asyncAssertSuccess(v -> channel1.producer().send("Hello")));
        });
    }

    @Test public void testSendStringRoundRobin(TestContext context)
    {
        final Async done = context.async();
        TestModule.run(channels -> {
            final Channel<String>
                channel1 = channels.channel("testSendStringRoundRobin", sendOptions().setEcho(true)),
                channel2 = channels.channel("testSendStringRoundRobin", sendOptions());
            final Future<Void>
                channel1Connected = Future.future(), channel2Connected = Future.future(),
                channel1Received = Future.future(), channel2Received = Future.future();

            channel1.consumer()
                .handler(msg -> channel1Received.complete())
                .completionHandler(channel1Connected);

            channel2.consumer()
                .handler(msg -> channel2Received.complete())
                .completionHandler(channel2Connected);

            CompositeFuture.all(channel1Connected, channel2Connected)
                .setHandler(context.asyncAssertSuccess(v -> {
                    channel1.producer().send("Hello");
                    // Push the second send to next tick to be a bit more realistic
                    rule.vertx().runOnContext(v2 -> channel1.producer().send("Hello"));
                }));

            CompositeFuture.all(channel1Received, channel2Received).setHandler(f -> done.complete());
        });
    }

    @Test public void testReplyToSelf(TestContext context)
    {
        final Async done = context.async();
        TestModule.run(channels -> {
            final Channel<String> channel = channels.channel("testReplyToSelf",
                                                             sendOptions().setEcho(true));
            channel.consumer()
                .handler(msg -> msg.reply("World"))
                // Write the message when the consumer has been registered
                .completionHandler(context.asyncAssertSuccess(
                    v -> channel.producer().<String>send("Hello", context.asyncAssertSuccess(reply -> {
                        assertEquals("World", reply.body());
                        assertEquals("testReplyToSelf", reply.address());
                        assertTrue(reply.isSend());
                        done.complete();
                    }))));
        });
    }

    @Test public void testReplyToReply(TestContext context)
    {
        final Async done = context.async();
        TestModule.run(channels -> {
            final Channel<String> channel = channels.channel("testReplyToReply",
                                                             sendOptions().setEcho(true));
            channel.consumer()
                .handler(msg -> msg.reply("World", context.asyncAssertSuccess(reply -> {
                    assertEquals("!", reply.body());
                    assertEquals("testReplyToReply", reply.address());
                    done.complete();
                })))
                .completionHandler(context.asyncAssertSuccess(
                    v -> channel.producer().<String>send("Hello", context.asyncAssertSuccess(reply -> {
                        assertEquals("World", reply.body());
                        reply.reply("!");
                    }))));
        });
    }
}
