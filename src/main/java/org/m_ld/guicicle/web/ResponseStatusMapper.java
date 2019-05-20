/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.web;

import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.util.NoSuchElementException;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public interface ResponseStatusMapper
{
    HttpResponseStatus getStatusForError(Throwable error);

    ResponseStatusMapper DEFAULT = error -> {
        if (error instanceof IOException)
            return UNPROCESSABLE_ENTITY;
        if (error instanceof UnsupportedOperationException)
            return NOT_IMPLEMENTED;
        if (error instanceof NoSuchElementException)
            return NOT_FOUND;

        return INTERNAL_SERVER_ERROR;
    };
}
