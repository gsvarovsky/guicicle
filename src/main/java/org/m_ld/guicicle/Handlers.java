/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle;

import io.vertx.core.Handler;
import org.jetbrains.annotations.NotNull;

import java.util.*;

import static org.m_ld.guicicle.Handlers.Flag.SINGLE_INSTANCE;
import static org.m_ld.guicicle.Handlers.Flag.SINGLE_USE;

public class Handlers<T> extends AbstractCollection<Handler<T>> implements Handler<T>
{
    private final List<Handler<T>> handlers;
    private final Set<Flag> flags;

    public enum Flag
    {
        SINGLE_INSTANCE, SINGLE_USE
    }

    public Handlers()
    {
        this.flags = EnumSet.noneOf(Flag.class);
        this.handlers = new ArrayList<>();
    }

    public Handlers(Flag flag, Flag... flags)
    {
        this.flags = EnumSet.of(flag, flags);
        this.handlers = this.flags.contains(SINGLE_INSTANCE) ? new ArrayList<>(1) : new ArrayList<>();
    }

    @Override public void handle(T event)
    {
        handlers.forEach(h -> h.handle(event));
        if (flags.contains(SINGLE_USE))
            handlers.clear();
    }

    public void set(Handler<T> handler)
    {
        add(handler);
    }

    @Override public boolean add(Handler<T> handler)
    {
        if (flags.contains(SINGLE_INSTANCE) && !handlers.isEmpty())
            return handlers.set(0, handler) != handler;
        else
            return handlers.add(handler);
    }

    @NotNull @Override public Iterator<Handler<T>> iterator()
    {
        return handlers.iterator();
    }

    @Override public int size()
    {
        return handlers.size();
    }
}
