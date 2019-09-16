/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.eventbus.impl.CodecManager;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import org.m_ld.guicicle.TimerProvider.OneShot;
import org.m_ld.guicicle.TimerProvider.Periodic;
import org.m_ld.guicicle.channel.*;
import org.m_ld.guicicle.channel.ChannelProvider.Local;
import org.m_ld.guicicle.web.ResponseStatusMapper;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.google.common.collect.Maps.immutableEntry;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.name.Names.named;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;

public class VertxCoreModule extends AbstractModule
{
    private final Vertx vertx;
    private final String deploymentID;
    private final JsonObject config;

    VertxCoreModule(Vertx vertx, String deploymentID, JsonObject config)
    {
        this.vertx = vertx;
        this.deploymentID = deploymentID;
        this.config = config;
    }

    @Override protected void configure()
    {
        bind(String.class).annotatedWith(named("vertx.deploymentID")).toInstance(deploymentID);
        bindJsonValue("config", config);
        newSetBinder(binder(), ChannelCodec.class);
        newSetBinder(binder(), Vertice.class);
    }

    private void bindJsonValue(String key, Object value)
    {
        if (value instanceof JsonObject)
        {
            final JsonObject jsonObject = normaliseDeepKeys((JsonObject)value);
            bind(JsonObject.class).annotatedWith(named(key)).toInstance(jsonObject);
            bind(Map.class).annotatedWith(named(key)).toInstance(jsonObject.getMap());
            jsonObject.fieldNames().forEach(name -> bindJsonValue(key + '.' + name, jsonObject.getValue(name)));
        }
        else if (value instanceof JsonArray)
        {
            bind(JsonArray.class).annotatedWith(named(key)).toInstance(((JsonArray)value));
            bind(List.class).annotatedWith(named(key)).toInstance(((JsonArray)value).getList());
        }
        else if (value != null)
        {
            //noinspection unchecked
            bind((Class)value.getClass()).annotatedWith(named(key)).toInstance(value);
        }
    }

    private JsonObject normaliseDeepKeys(JsonObject jsonObject)
    {
        return new JsonObject(jsonObject.stream().map(e -> {
            final String[] split = e.getKey().split("\\.", 2);
            return split.length > 1 ? immutableEntry(split[0], new JsonObject().put(split[1], e.getValue())) : e;
        }).collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> {
            if (Objects.equals(v1, v2))
                return v1;
            if (v1 instanceof JsonObject && v2 instanceof JsonObject)
                return ((JsonObject)v1).mergeIn((JsonObject)v2);
            if (v1 instanceof JsonArray && v2 instanceof JsonArray)
                return ((JsonArray)v1).addAll((JsonArray)v2);
            if (v1 instanceof JsonArray)
                return ((JsonArray)v1).add(v2);
            if (v2 instanceof JsonArray)
                return new JsonArray(singletonList(v1)).addAll((JsonArray)v2);
            return new JsonArray().add(v1).add(v2);
        })));
    }

    @Provides @Singleton Vertx vertx(Set<ChannelCodec> codecs)
    {
        final LocalMap<String, String> regCodecs = vertx.sharedData().getLocalMap("guicicle.codecs");
        new CodecRegistrar()
        {
            @Override void registerCodec(ChannelCodec codec)
            {
                if (codec.isDefault())
                    //noinspection unchecked
                    vertx.eventBus().registerDefaultCodec(codec.getDataClass(), codec);
                else
                    vertx.eventBus().registerCodec(codec);

                regCodecs.put(codec.name(), codec.getDataClass().getName());
            }

            @Override String getRegisteredCodecDataClassName(String codecName)
            {
                return regCodecs.get(codecName);
            }
        }.registerCodecs(codecs);
        return vertx;
    }

    @Provides CodecManager codecManager(Set<ChannelCodec> codecs)
    {
        final CodecManager codecManager = new CodecManager();
        new CodecRegistrar()
        {
            @Override void registerCodec(ChannelCodec codec)
            {
                if (codec.isDefault())
                    //noinspection unchecked
                    codecManager.registerDefaultCodec(codec.getDataClass(), codec);
                else
                    codecManager.registerCodec(codec);
            }

            @Override String getRegisteredCodecDataClassName(String codecName)
            {
                final MessageCodec existing = codecManager.getCodec(codecName);
                if (existing != null)
                {
                    if (existing instanceof ChannelCodec)
                        return ((ChannelCodec)existing).getDataClass().getName();
                    else
                        return "<non-channel data>";
                }
                else
                    return null;
            }
        }.registerCodecs(codecs);
        return codecManager;
    }

    private abstract static class CodecRegistrar
    {
        abstract void registerCodec(ChannelCodec codec);

        abstract String getRegisteredCodecDataClassName(String codecName);

        void registerCodecs(Set<ChannelCodec> codecs)
        {
            codecs.forEach(codec -> {
                final Class dataClass = codec.getDataClass();
                final String existingDataClassName = getRegisteredCodecDataClassName(codec.name());
                if (existingDataClassName != null)
                {
                    if (!existingDataClassName.equals(dataClass.getName()))
                        throw new IllegalStateException(format(
                            "Codec already registered for name %s with different data %s",
                            codec.name(), existingDataClassName));
                    // Otherwise do nothing - we already have a codec for the right data class with the given name
                }
                else
                {
                    registerCodec(codec);
                }
            });
        }
    }

    @Provides EventBus eventBus(Vertx vertx)
    {
        return vertx.eventBus();
    }

    @Provides @Local ChannelProvider eventBusChannels(Vertx vertx, ResponseStatusMapper statusMapper)
    {
        return new ChannelProvider()
        {
            @Override public <T> Channel<T> channel(String address, ChannelOptions options)
            {
                return new EventBusChannel<>(vertx.eventBus(), address, options);
            }
        };
    }

    @Provides @Blocking Executor blockingExecutor(Vertx vertx)
    {
        return runnable -> vertx.executeBlocking(future -> {
            runnable.run();
            future.complete(null);
        }, result -> {
            if (result.failed())
                vertx.exceptionHandler().handle(result.cause());
        });
    }

    @Provides @Periodic TimerProvider periodicTimerProvider(Vertx vertx)
    {
        return new TimerProvider()
        {
            @Override public long setTimer(long delay, Handler<Long> handler)
            {
                return vertx.setPeriodic(delay, handler);
            }

            @Override public boolean cancelTimer(long id)
            {
                return vertx.cancelTimer(id);
            }
        };
    }

    @Provides @OneShot TimerProvider oneShotTimerProvider(Vertx vertx)
    {
        return new TimerProvider()
        {
            @Override public long setTimer(long delay, Handler<Long> handler)
            {
                return vertx.setTimer(delay, handler);
            }

            @Override public boolean cancelTimer(long id)
            {
                return vertx.cancelTimer(id);
            }
        };
    }

    @ProvidesIntoSet ChannelCodec uuidCodec()
    {
        return new UuidCodec();
    }

    @Provides ResponseStatusMapper defaultResponseStatusMapper()
    {
        return ResponseStatusMapper.DEFAULT;
    }
}
