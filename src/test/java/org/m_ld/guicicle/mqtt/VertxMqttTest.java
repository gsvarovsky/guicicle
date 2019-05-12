/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import com.google.inject.multibindings.ProvidesIntoSet;
import io.moquette.broker.Server;
import io.moquette.broker.config.ClasspathResourceLoader;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.IResourceLoader;
import io.moquette.broker.config.ResourceLoaderConfig;
import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.mqtt.messages.MqttPublishMessage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.m_ld.guicicle.Guicicle;
import org.m_ld.guicicle.Vertice;
import org.m_ld.guicicle.channel.ChannelProvider;
import org.m_ld.guicicle.channel.ChannelProvider.Central;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertNotNull;

@RunWith(VertxUnitRunner.class)
public class VertxMqttTest
{
    @ClassRule public static RunTestOnContext rule = new RunTestOnContext();
    private static Server mqttBroker;
    private static final AtomicReference<Future<MqttPublishMessage>> published = new AtomicReference<>();

    public static class TestModule extends VertxMqttModule
    {
        private static final AtomicReference<Consumer<ChannelProvider>> test = new AtomicReference<>();

        @ProvidesIntoSet Vertice puppeteerVertice(@Central ChannelProvider channels, EventBus eventBus)
        {
            return new Vertice()
            {
                @Override public void start()
                {
                    eventBus.<Void>consumer("run.test", v -> test.get().accept(channels));
                }
            };
        }

        static void run(Consumer<ChannelProvider> theTest)
        {
            test.set(theTest);
            rule.vertx().eventBus().publish("run.test", null);
        }
    }

    @BeforeClass public static void setUp(TestContext context) throws IOException
    {
        IResourceLoader classpathLoader = new ClasspathResourceLoader();
        final IConfig classPathConfig = new ResourceLoaderConfig(classpathLoader);

        mqttBroker = new Server();
        mqttBroker.startServer(classPathConfig, singletonList(new AbstractInterceptHandler()
        {
            @Override
            public String getID()
            {
                return "ServerPublishListener";
            }

            @Override
            public void onPublish(InterceptPublishMessage msg)
            {
                published.get().complete(MqttPublishMessage.create(
                    -1, msg.getQos(), msg.isDupFlag(), msg.isRetainFlag(), msg.getTopicName(), msg.getPayload()));
            }
        }));

        final Async deployed = context.async();
        final DeploymentOptions options = new DeploymentOptions().setConfig(
            new JsonObject()
                .put("guice.module", TestModule.class.getName())
                .put("mqtt", new JsonObject())); // All default options

        rule.vertx().deployVerticle(Guicicle.class, options, s -> deployed.complete());
    }

    @Test public void testTestSetup(TestContext context)
    {
        final Async done = context.async();
        TestModule.run(channels -> {
            assertNotNull(channels);
            done.complete();
        });
    }

    @AfterClass public static void tearDown()
    {
        // Shut down Vert.x first to suppress dead sockets when disconnecting the MQTT Vertice
        rule.vertx().close(v -> mqttBroker.stopServer());
    }
}
