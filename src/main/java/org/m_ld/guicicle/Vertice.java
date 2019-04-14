/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle;

import io.vertx.core.Future;

/**
 * A "Vert(.x Serv)ice" is an injectable Verticle-like singleton that is {@link #start}ed and {@link #stop}ped from a
 * single controlling Guicicle.
 * <p>
 * Vertices are not started in any specific order, so in the case of inter-dependencies, they must be tolerant of
 * un-started dependencies such as by providing a start listener.
 * <p>
 * This interface is simplified from {@link io.vertx.core.AbstractVerticle} and so duplicates some of its documentation.
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
     * @see io.vertx.core.AbstractVerticle#start(Future)
     */
    default void start(Future<Void> startFuture)
    {
        start();
        startFuture.complete();
    }

    /**
     * Stop the vertice instance.
     * <p>
     * A future is passed into the method, and when un-deployment is complete the verticle should either call
     * {@link io.vertx.core.Future#complete} or {@link io.vertx.core.Future#fail} the future.
     *
     * @param stopFuture the future
     * @see io.vertx.core.AbstractVerticle#stop(Future)
     */
    default void stop(Future<Void> stopFuture)
    {
        stop();
        stopFuture.complete();
    }

    /**
     * If your vertice does a simple, synchronous start-up then override this method and put your start-up
     * code in here.
     *
     * @see io.vertx.core.AbstractVerticle#start()
     */
    default void start()
    {
    }

    /**
     * If your verticle has simple synchronous clean-up tasks to complete then override this method and put your
     * clean-up code in here.
     *
     * @see io.vertx.core.AbstractVerticle#stop()
     */
    default void stop()
    {
    }
}