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
import io.vertx.core.eventbus.impl.CodecManager;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.mqtt.messages.MqttPublishMessage;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.m_ld.guicicle.Guicicle;
import org.m_ld.guicicle.Vertice;
import org.m_ld.guicicle.channel.ChannelProvider;
import org.m_ld.guicicle.channel.ManagedCodecs;
import org.m_ld.guicicle.channel.MessageCodecs;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Collections.singletonList;

@RunWith(VertxUnitRunner.class)
public abstract class VertxMqttWithBrokerTest
{
    @ClassRule public static RunTestOnContext rule = new RunTestOnContext();
    private static Server mqttBroker;
    static final AtomicReference<Future<MqttPublishMessage>> published = new AtomicReference<>();
    static final MessageCodecs EVENT_CODEC = new ManagedCodecs(new CodecManager());

    @SuppressWarnings("WeakerAccess") public static class TestModule extends VertxMqttModule
    {
        private static final AtomicReference<Consumer<ChannelProvider>> test = new AtomicReference<>();

        @Override protected void configure()
        {
            super.configure();
            bind(MessageCodecs.class).to(ManagedCodecs.class);
        }

        @ProvidesIntoSet Vertice puppeteerVertice(@ChannelProvider.Central ChannelProvider channels, EventBus eventBus)
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
            published.set(Future.future());
            test.set(theTest);
            rule.vertx().eventBus().publish("run.test", null);
        }
    }

    public static void setUp(TestContext context, JsonObject mqttOptions) throws IOException
    {
        IResourceLoader classpathLoader = new ClasspathResourceLoader();
        final IConfig classPathConfig = new ResourceLoaderConfig(classpathLoader);

        if (mqttBroker == null)
        {
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
                        -1, msg.getQos(), msg.isDupFlag(), msg.isRetainFlag(),
                        msg.getTopicName(), msg.getPayload().copy()));
                    published.set(Future.future());
                }
            }));
        }

        final Async deployed = context.async();
        final DeploymentOptions options = new DeploymentOptions().setConfig(
            new JsonObject()
                .put("guice.module", TestModule.class.getName())
                .put("mqtt", mqttOptions)); // All default options (no presence)

        rule.vertx().deployVerticle(Guicicle.class, options, s -> deployed.complete());
    }
}
