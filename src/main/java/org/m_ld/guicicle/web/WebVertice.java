/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.web;

import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import org.m_ld.guicicle.Vertice;

import javax.inject.Inject;
import java.util.Map;
import java.util.logging.Logger;

import static java.lang.String.format;

public class WebVertice implements Vertice
{
    private static final Logger LOG = Logger.getLogger(WebVertice.class.getName());
    private final HttpServer httpServer;
    private final Router router;

    @Inject public WebVertice(HttpServer httpServer, Router router, Map<String, Router> subRouters)
    {
        this.httpServer = httpServer;
        this.router = router;
        subRouters.forEach(router::mountSubRouter);
    }

    @Override public void start(Future<Void> startFuture)
    {
        httpServer.requestHandler(router).listen(serverResult -> {
            if (serverResult.failed())
                startFuture.fail(serverResult.cause());
            else
            {
                LOG.info(format("HTTP server started on port %d", serverResult.result().actualPort()));
                startFuture.complete();
            }
        });
    }

    @Override public void stop(Future<Void> stopFuture)
    {
        httpServer.close(stopFuture);
    }

    public Router router()
    {
        return router;
    }
}
