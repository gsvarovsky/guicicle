/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoSet;
import com.google.inject.name.Names;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.m_ld.guicicle.channel.*;
import org.m_ld.guicicle.channel.ChannelProvider.Local;

import javax.inject.Named;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.lang.reflect.Modifier.isFinal;

public class VertxCoreModule extends AbstractModule
{
    private final Vertx vertx;
    private final JsonObject config;

    VertxCoreModule(Vertx vertx, JsonObject config)
    {
        this.vertx = vertx;
        this.config = config;
    }

    @Override protected void configure()
    {
        bindJsonValue("config", config);
        newSetBinder(binder(), ChannelCodec.class);
        newSetBinder(binder(), Vertice.class);
    }

    private void bindObjectFields(String prefix, JsonObject object)
    {
        object.fieldNames().forEach(
            key -> bindJsonValue(prefix.isEmpty() ? key : prefix + '.' + key, object.getValue(key)));
    }

    private void bindJsonValue(String key, Object value)
    {
        if (value instanceof JsonObject)
        {
            bindObjectFields(key, (JsonObject)value);
            bind(JsonObject.class).annotatedWith(Names.named(key)).toInstance(((JsonObject)value));
            bind(Map.class).annotatedWith(Names.named(key)).toInstance(((JsonObject)value).getMap());
        }
        else if (value instanceof JsonArray)
        {
            bind(JsonArray.class).annotatedWith(Names.named(key)).toInstance(((JsonArray)value));
            bind(List.class).annotatedWith(Names.named(key)).toInstance(((JsonArray)value).getList());
        }
        else if (value != null)
        {
            //noinspection unchecked
            bind((Class)value.getClass()).annotatedWith(Names.named(key)).toInstance(value);
        }
    }

    @Provides @Singleton Vertx vertx(Set<ChannelCodec> codecs)
    {
        codecs.forEach(codec -> {
            final Class dataClass = codec.getDataClass();
            if (isFinal(dataClass.getModifiers()))
            {
                //noinspection unchecked
                vertx.eventBus().registerDefaultCodec(dataClass, codec);
            }
            else
            {
                vertx.eventBus().registerCodec(codec);
            }
        });
        return vertx;
    }

    @Provides HttpServerOptions httpServerOptions(@Named("config.http") JsonObject httpOptions)
    {
        return new HttpServerOptions(httpOptions);
    }

    @Provides HttpServer httpServer(HttpServerOptions httpServerOptions)
    {
        return vertx.createHttpServer(httpServerOptions);
    }

    @Provides @Local ChannelProvider eventBusChannels(Vertx vertx)
    {
        return new ChannelProvider()
        {
            @Override public <T> Channel<T> channel(String address, ChannelOptions options)
            {
                return new EventBusChannel<>(vertx, address, options);
            }
        };
    }

    @Provides Executor blockingExecutor(Vertx vertx)
    {
        return runnable -> vertx.executeBlocking(future -> {
            runnable.run();
            future.complete(null);
        }, result -> {
            if (result.failed())
                vertx.exceptionHandler().handle(result.cause());
        });
    }

    @ProvidesIntoSet ChannelCodec uuidCodec()
    {
        return new UuidCodec();
    }
}
