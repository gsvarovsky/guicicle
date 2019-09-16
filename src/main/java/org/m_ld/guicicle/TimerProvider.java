/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle;

import com.google.inject.BindingAnnotation;
import io.vertx.core.Handler;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Timer functions from {@link io.vertx.core.Vertx}.
 */
public interface TimerProvider
{
    /**
     * Marks a timer as one-shot
     */
    @BindingAnnotation @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME) @interface OneShot {}

    /**
     * Marks a timer as periodic
     */
    @BindingAnnotation @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME) @interface Periodic {}

    /**
     * Set a timer to fire after {@code delay} milliseconds, at which point {@code handler} will be called with
     * the id of the timer.
     *
     * @param delay   the delay in milliseconds, after which the timer will fire
     * @param handler the handler that will be called with the timer ID when the timer fires
     * @return the unique ID of the timer
     */
    long setTimer(long delay, Handler<Long> handler);

    /**
     * Cancels the timer with the specified {@code id}.
     *
     * @param id The id of the timer to cancel
     * @return true if the timer was successfully cancelled, or false if the timer does not exist.
     */
    boolean cancelTimer(long id);
}
