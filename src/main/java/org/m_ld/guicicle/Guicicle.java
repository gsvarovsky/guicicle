/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.shareddata.Shareable;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static io.vertx.core.CompositeFuture.all;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public class Guicicle extends AbstractVerticle
{
    @Inject private Set<Vertice> vertices;

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

    @Override public void start(Future<Void> startFuture)
    {
        forEachVertice(startFuture, Vertice::start);
    }

    @Override public void stop(Future<Void> stopFuture)
    {
        forEachVertice(stopFuture, Vertice::stop);
    }

    private void forEachVertice(Future<Void> doneFuture, BiConsumer<Vertice, Future<Void>> action)
    {
        all(vertices.stream().map(vertice -> {
            final Future<Void> future = Future.future();
            action.accept(vertice, future);
            return future;
        }).collect(toList())).setHandler(result -> {
            if (result.failed())
            {
                doneFuture.fail(result.cause());
            }
            else
                doneFuture.complete();
        });
    }
}
