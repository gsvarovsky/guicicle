/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m_ld.guicicle.channel.Channel;
import org.m_ld.guicicle.channel.ChannelOptions;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VertxMqttSendTest extends VertxMqttTest
{
    private static JsonObject mqttOptions = new JsonObject().put("hasPresence", true);

    @BeforeClass public static void setUp(TestContext context) throws IOException
    {
        VertxMqttTest.setUp(context, mqttOptions);
    }

    @Test public void testSendStringToSelf(TestContext context)
    {
        final Async done = context.async();
        TestModule.run(channels -> {
            final Channel<String> channel = channels.channel("testSendStringToSelf",
                                                             new ChannelOptions().setEcho(true));
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
}
