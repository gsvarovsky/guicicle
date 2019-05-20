/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.web;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import org.m_ld.guicicle.Vertice;

import javax.inject.Named;

import static com.google.inject.multibindings.MapBinder.newMapBinder;

public class VertxWebModule extends AbstractModule
{
    @Override protected void configure()
    {
        newMapBinder(binder(), String.class, Router.class);
        bind(WebVertice.class).asEagerSingleton();
    }

    @Provides HttpServerOptions httpServerOptions(@Named("config.http") JsonObject httpOptions)
    {
        return new HttpServerOptions(httpOptions);
    }

    @Provides HttpServer httpServer(Vertx vertx, HttpServerOptions httpServerOptions)
    {
        return vertx.createHttpServer(httpServerOptions);
    }

    @Provides Router router(Vertx vertx)
    {
        return Router.router(vertx);
    }

    @ProvidesIntoSet Vertice webVertice(WebVertice webVertice)
    {
        return webVertice;
    }
}
