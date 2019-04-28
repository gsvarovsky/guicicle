/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.http;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;

import javax.inject.Named;

public class VertxHttpModule extends AbstractModule
{
    @Provides HttpServerOptions httpServerOptions(@Named("config.http") JsonObject httpOptions)
    {
        return new HttpServerOptions(httpOptions);
    }

    @Provides HttpServer httpServer(Vertx vertx, HttpServerOptions httpServerOptions)
    {
        return vertx.createHttpServer(httpServerOptions);
    }
}
