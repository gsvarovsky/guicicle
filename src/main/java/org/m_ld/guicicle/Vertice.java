/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle;

import io.vertx.core.Future;

/**
 * A "Vert(.x Serv)ice" is an injectable Verticle-like singleton that is {@link #start}ed and {@link #stop}ped from a
 * single controlling Guicicle.
 */
public interface Vertice
{
    /**
     * Start the vertice instance.
     * <p>
     * A future is passed into the method, and when deployment is complete the verticle should either call
     * {@link io.vertx.core.Future#complete} or {@link io.vertx.core.Future#fail} the future.
     *
     * @param startFuture the future
     */
    void start(Future<Void> startFuture);

    /**
     * Stop the vertice instance.
     * <p>
     * A future is passed into the method, and when un-deployment is complete the verticle should either call
     * {@link io.vertx.core.Future#complete} or {@link io.vertx.core.Future#fail} the future.
     *
     * @param stopFuture the future
     */
    void stop(Future<Void> stopFuture);
}