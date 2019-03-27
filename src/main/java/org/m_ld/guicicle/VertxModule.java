package org.m_ld.guicicle;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;
import org.m_ld.guicicle.codec.BidiCodec;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static java.lang.reflect.Modifier.isFinal;
import static java.util.Collections.emptyList;

public class VertxModule extends AbstractModule
{
    private final Vertx vertx;
    private final JsonObject config;

    VertxModule(Vertx vertx, JsonObject config)
    {
        this.vertx = vertx;
        this.config = config;
    }

    @Override
    protected void configure()
    {
        bindObjectFields("", config);
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
            bindObjectFields(key, (JsonObject) value);
            bind(Map.class).annotatedWith(Names.named(key)).toInstance(((JsonObject) value).getMap());
        }
        else if (value instanceof JsonArray)
        {
            bind(List.class).annotatedWith(Names.named(key)).toInstance(((JsonArray) value).getList());
        }
        else if (value != null)
        {
            //noinspection unchecked
            bind((Class) value.getClass()).annotatedWith(Names.named(key)).toInstance(value);
        }
    }

    private static class Codecs implements Iterable<BidiCodec>
    {
        @Inject(optional = true)
        List<BidiCodec> codecs = emptyList();

        @NotNull @Override
        public Iterator<BidiCodec> iterator()
        {
            return codecs.iterator();
        }
    }

    @Provides
    @Named("config")
    JsonObject config()
    {
        return config;
    }

    @Provides
    @Singleton
    Vertx vertx(Codecs codecs)
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

    @Provides
    Executor blockingExecutor(Vertx vertx)
    {
        return runnable -> vertx.executeBlocking(future -> {
            runnable.run();
            future.complete(null);
        }, result -> {
            if (result.failed())
                vertx.exceptionHandler().handle(result.cause());
        });
    }
}
