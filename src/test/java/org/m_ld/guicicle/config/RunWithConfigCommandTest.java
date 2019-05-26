/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.config;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.impl.launcher.VertxLifecycleHooks;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.launcher.ExecutionContext;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.*;

@RunWith(VertxUnitRunner.class)
public class RunWithConfigCommandTest
{
    @Rule public RunTestOnContext rule = new RunTestOnContext();

    public static class TestVerticle extends AbstractVerticle
    {
        @Override public void start()
        {
            vertx.eventBus().publish("test.config", config());
        }

        static void whenConfigured(Vertx vertx, Handler<JsonObject> configured)
        {
            final MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer("test.config");
            consumer.handler(msg -> {
                configured.handle(msg.body());
                consumer.unregister();
            });
        }
    }

    @Test public void testNoConfigConfigDeploy(TestContext context)
    {
        final Async done = context.async();
        final JsonObject config = new JsonObject().put("a", "b");
        final RunWithConfigCommand run = new RunWithConfigCommand();
        final ExecutionContext execContext = mock(ExecutionContext.class);
        final VertxLifecycleHooks lifecycleHooks = mock(VertxLifecycleHooks.class);
        TestVerticle.whenConfigured(rule.vertx(), c -> {
            context.assertEquals(config, c);
            final ArgumentCaptor<DeploymentOptions> doCapture = ArgumentCaptor.forClass(DeploymentOptions.class);
            verify(lifecycleHooks).beforeDeployingVerticle(doCapture.capture());
            context.assertEquals(config, doCapture.getValue().getConfig());
            done.complete();
        });
        when(execContext.main()).thenReturn(lifecycleHooks);
        run.setExecutionContext(execContext);
        run.deploy(TestVerticle.class.getName(),
                   rule.vertx(),
                   new DeploymentOptions().setConfig(config),
                   context.asyncAssertSuccess());
    }

    @Test public void testDefaultConfigConfigDeploy(TestContext context)
    {
        final Async done = context.async();
        final JsonObject config = new JsonObject().put("a", "b")
            .put("config", new JsonObject().put("includeDefaultStores", true));
        TestVerticle.whenConfigured(rule.vertx(), c -> {
            // Maintains passed-in config
            context.assertEquals("b", c.getString("a"));
            context.assertFalse(c.containsKey("config"));
            // Has a load of Java keys from System.getProperties
            context.assertTrue(c.stream().anyMatch(e -> e.getKey().startsWith("java.")));
            // Has environment variables
            context.assertTrue(c.containsKey("PATH"));
            done.complete();
        });
        final RunWithConfigCommand run = new RunWithConfigCommand();
        run.setExecutionContext(mock(ExecutionContext.class));
        run.deploy(TestVerticle.class.getName(),
                   rule.vertx(),
                   new DeploymentOptions().setConfig(config),
                   context.asyncAssertSuccess());
    }

    @Test public void testCustomConfigConfigDeploy(TestContext context)
    {
        final Async done = context.async();
        final JsonObject config = new JsonObject()
            .put("a", "b")
            .put("config", new JsonObject()
                .put("stores", new JsonArray()
                    .add(new JsonObject()
                             .put("type", "json")
                             .put("config", new JsonObject().put("c", "d")))));
        TestVerticle.whenConfigured(rule.vertx(), c -> {
            context.assertEquals("b", c.getString("a"));
            context.assertEquals("d", c.getString("c"));
            context.assertFalse(c.containsKey("config"));
            done.complete();
        });
        final RunWithConfigCommand run = new RunWithConfigCommand();
        run.setExecutionContext(mock(ExecutionContext.class));
        run.deploy(TestVerticle.class.getName(),
                   rule.vertx(),
                   new DeploymentOptions().setConfig(config),
                   context.asyncAssertSuccess());
    }

    @Test public void testRecursiveConfigConfigDeploy(TestContext context)
    {
        final Async done = context.async();
        final JsonObject config = new JsonObject()
            .put("a", "b")
            .put("config", new JsonObject()
                .put("stores", new JsonArray()
                    .add(new JsonObject()
                             .put("type", "json")
                             .put("config", new JsonObject().put("config", new JsonObject()
                                 .put("stores", new JsonArray()
                                     .add(new JsonObject()
                                              .put("type", "json")
                                              .put("config", new JsonObject().put("c", "d")))))))));
        TestVerticle.whenConfigured(rule.vertx(), c -> {
            context.assertEquals("b", c.getString("a"));
            context.assertEquals("d", c.getString("c"));
            context.assertFalse(c.containsKey("config"));
            done.complete();
        });
        final RunWithConfigCommand run = new RunWithConfigCommand();
        run.setExecutionContext(mock(ExecutionContext.class));
        run.deploy(TestVerticle.class.getName(),
                   rule.vertx(),
                   new DeploymentOptions().setConfig(config),
                   context.asyncAssertSuccess());
    }
}