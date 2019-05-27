/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.web;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JsonErrorHandler implements Handler<RoutingContext>
{
    private static final Logger LOG = Logger.getLogger(JsonErrorHandler.class.getName());
    private final ResponseStatusMapper statusMapper;

    @Inject public JsonErrorHandler(ResponseStatusMapper statusMapper)
    {
        this.statusMapper = statusMapper;
    }

    @Override public void handle(RoutingContext context)
    {
        if (context.failed() && context.failure() != null)
        {
            final HttpResponseStatus status = statusMapper.getStatusForError(context.failure());
            LOG.log(status.code() < 500 ? Level.FINE : Level.WARNING, status.reasonPhrase(), context.failure());
            String cause = context.failure().getMessage();

            final JsonObject body = new JsonObject()
                .put("status", status.code())
                .put("reason", status.reasonPhrase());

            if (cause != null && status.code() < 500)
                body.put("cause", cause);

            context.response()
                .setStatusCode(status.code())
                .putHeader("content-type", "application/json")
                .end(body.encodePrettily());
        }
    }
}
