/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.m_ld.guicicle.channel.ChannelOptions.Delivery;

import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class EventBusChannelTest
{
    @Rule public RunTestOnContext rule = new RunTestOnContext();

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedQualityOfService()
    {
        new EventBusChannel<String>(rule.vertx().eventBus(), "test", new ChannelOptions().setQuality(
            ChannelOptions.Quality.AT_LEAST_ONCE));
    }

    @Test public void testPublishIsProduced(TestContext context)
    {
        final Async produced = context.async();

        final EventBusChannel<String> channel = new EventBusChannel<>(
            rule.vertx().eventBus(), "test", new ChannelOptions().setDelivery(Delivery.PUBLISH));

        channel.producedHandler(context.asyncAssertSuccess(body -> {
            assertEquals("Hello", body);
            produced.complete();
        }));

        channel.producer().write("Hello");
    }

    @Test public void testPublishIsNotEchoed(TestContext context)
    {
        final Async received = context.async();

        final EventBusChannel<String> channel = new EventBusChannel<>(
            rule.vertx().eventBus(), "test", new ChannelOptions().setDelivery(Delivery.PUBLISH).setEcho(false));

        rule.vertx().eventBus().consumer("test", msg -> {
            assertEquals("Hello", msg.body());
            // Push the receipt to the next tick so that the echo has the chance to land
            rule.vertx().runOnContext(v -> received.complete());
        });
        channel.consumer().handler(msg -> context.fail());
        channel.producer().write("Hello");
    }

    @Test public void testPublishIsEchoed(TestContext context)
    {
        final Async received = context.async();

        final EventBusChannel<String> channel = new EventBusChannel<>(
            rule.vertx().eventBus(), "test", new ChannelOptions().setDelivery(Delivery.PUBLISH).setEcho(true));

        channel.consumer().handler(msg -> {
            assertEquals("Hello", msg.body());
            received.complete();
        });
        channel.producer().write("Hello");
    }

    @Test public void testSendEchoIsIgnored(TestContext context)
    {
        final int sendCount = 3;
        final Async received = context.async(sendCount);

        final EventBusChannel<Integer> channel = new EventBusChannel<>(
            rule.vertx().eventBus(), "test", new ChannelOptions().setDelivery(Delivery.SEND).setEcho(false));

        channel.consumer().handler(msg -> context.fail());
        range(0, sendCount).forEach(n -> channel.producer().write(n));

        rule.vertx().setTimer(1, v -> received.complete());
    }

    @Test public void testSendIsNotEchoed(TestContext context)
    {
        final int sendCount = 3;
        final Async received = context.async(sendCount);

        final EventBusChannel<Integer> channel = new EventBusChannel<>(
            rule.vertx().eventBus(), "test", new ChannelOptions().setDelivery(Delivery.SEND).setEcho(false));

        // We expect all sends to go to this non-channel consumer
        rule.vertx().eventBus().<Integer>consumer("test", msg -> {
            assertTrue(msg.body() >= 0);
            assertTrue(msg.body() < sendCount);
            received.countDown();
        });
        // If our consumer gets any of the messages, fail
        channel.consumer().handler(msg -> context.fail());
        range(0, sendCount).forEach(n -> channel.producer().write(n));
    }

    @Test public void testSendWithReplyNotEchoed(TestContext context)
    {
        final int sendCount = 3;
        final Async received = context.async(sendCount);

        final EventBusChannel<Integer> channel = new EventBusChannel<>(
            rule.vertx().eventBus(), "test", new ChannelOptions().setDelivery(Delivery.SEND).setEcho(false));

        // We expect all sends to go to this non-channel consumer, which replies with the sent integer
        rule.vertx().eventBus().<Integer>consumer("test", msg -> msg.reply(msg.body()));
        // If our consumer gets any of the messages, fail
        channel.consumer().handler(msg -> context.fail());
        range(0, sendCount).forEach(n -> channel.producer().send(n, context.asyncAssertSuccess(reply -> {
            assertEquals(n, reply.body());
            received.countDown();
        })));
    }
}