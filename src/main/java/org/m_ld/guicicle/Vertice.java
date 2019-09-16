/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle;

import io.vertx.core.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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
        //noinspection deprecation
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
        //noinspection deprecation
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

    /**
     * Converts a Vert.x fluent-style asynchronous API call to Promise-style, returning a {@code Future} and discarding
     * the redundant current object return value.
     *
     * @param fluentApi the fluent API, normally returning the callee.
     * @param <T>       the asynchronous result type
     * @return a {@code Future} (promise) of the API result
     */
    static <T> Future<T> when(Consumer<Handler<AsyncResult<T>>> fluentApi)
    {
        final Future<T> future = Promise.<T>promise().future();
        fluentApi.accept(future);
        return future;
    }

    /**
     * Converts a Vert.x fluent-style asynchronous API call to Promise-style, returning a {@code Future} and discarding
     * the redundant current object return value.
     *
     * @param fluentApi the fluent API, normally returning the callee.
     * @param param     the parameter of the fluent API call
     * @param <P>       the parameter type
     * @param <T>       the asynchronous result type
     * @return a {@code Future} (promise) of the API result
     */
    static <P, T> Future<T> when(BiConsumer<P, Handler<AsyncResult<T>>> fluentApi, P param)
    {
        final Future<T> future = Promise.<T>promise().future();
        fluentApi.accept(param, future);
        return future;
    }

    /**
     * Converts a Vert.x future to a Java util completion stage. The returned stage will be completed on the Vert.x
     * event thread.
     *
     * @param future the Vert.x future
     * @param <T>    the result type
     * @return a Java util completion stage
     */
    static <T> CompletionStage<T> fromFuture(Future<T> future)
    {
        final CompletableFuture<T> completableFuture = new CompletableFuture<>();
        future.setHandler(result -> {
            if (result.succeeded())
                completableFuture.complete(result.result());
            else
                completableFuture.completeExceptionally(result.cause());
        });
        return completableFuture;
    }

    /**
     * Converts a Java util completion stage into a Vert.x future. The future is guaranteed to be completed on the
     * Vert.x event thread.
     *
     * @param stage the Java util completion stage
     * @param vertx the Vert.x instance
     * @param <T> the result type
     * @return a Vert.x future
     */
    static <T> Future<T> toFuture(CompletionStage<T> stage, Vertx vertx)
    {
        final Promise<T> promise = Promise.promise();
        stage.whenComplete((value, error) -> vertx.runOnContext(v -> {
            if (error == null)
                promise.complete(value);
            else
                promise.fail(error);
        }));
        return promise.future();
    }
}