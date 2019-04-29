/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Module;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import org.jetbrains.annotations.NotNull;

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

    @Override public void init(Vertx vertx, Context context)
    {
        super.init(vertx, context);
        try
        {
            Guice.createInjector(modules()).injectMembers(this);
        }
        catch (RuntimeException e)
        {
            // This exception never gets printed in Vert.x
            e.printStackTrace();
            throw e;
        }
    }

    @NotNull private Module[] modules()
    {
        return concat(Stream.of(new VertxCoreModule(vertx, context.deploymentID(), config())),
                      appModules()).toArray(Module[]::new);
    }

    @NotNull private Stream<Module> appModules()
    {
        return appModuleClassNames().map(m -> {
            try
            {
                return (Module)Class.forName(m).newInstance();
            }
            catch (ClassNotFoundException | IllegalAccessException | InstantiationException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    private Stream<String> appModuleClassNames()
    {
        return config().getJsonArray("guice.modules", new JsonArray(
            singletonList(config().getString("guice.module")))).stream().map(Object::toString);
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
        }).collect(toList())).setHandler(result -> doneFuture.handle(result.mapEmpty()));
    }
}
