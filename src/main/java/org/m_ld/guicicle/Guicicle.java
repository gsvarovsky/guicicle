package org.m_ld.guicicle;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.shareddata.Shareable;

import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.stream.Stream.concat;

public abstract class Guicicle extends AbstractVerticle
{
    private static class SharedInjector implements Shareable
    {
        final Injector injector;

        SharedInjector(Stream<Module> modules)
        {
            this.injector = Guice.createInjector(modules.toArray(Module[]::new));
        }
    }

    @Override
    public void init(Vertx vertx, Context context)
    {
        super.init(vertx, context);

        try
        {
            vertx.sharedData().<String, SharedInjector>getLocalMap("guicicle")
                .computeIfAbsent("injector", k -> {
                    final Stream<Module> appModules =
                        config().getJsonArray("guice.modules", new JsonArray(
                            singletonList(config().getString("guice.module")))).stream().map(m -> {
                            try
                            {
                                return (Module)Class.forName(m.toString()).newInstance();
                            }
                            catch (ClassNotFoundException | IllegalAccessException | InstantiationException e)
                            {
                                throw new RuntimeException(e);
                            }
                        });
                    return new SharedInjector(concat(Stream.of(new VertxModule(vertx, config())), appModules));
                })
                .injector.injectMembers(this);
        }
        catch (RuntimeException e)
        {
            // This exception never gets printed in Vert.x
            e.printStackTrace();
            throw e;
        }
    }
}
