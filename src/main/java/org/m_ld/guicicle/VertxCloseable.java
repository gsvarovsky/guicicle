/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface VertxCloseable
{
    void addCloseHandler(Handler<AsyncResult<Void>> endHandler);
    void close();
}
