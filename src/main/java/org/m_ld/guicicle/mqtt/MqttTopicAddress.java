/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import java.util.*;

import static java.lang.String.join;
import static java.util.Arrays.asList;
import static java.util.Arrays.copyOfRange;

public abstract class MqttTopicAddress<T extends MqttTopicAddress<T>> extends AbstractList<String>
{
    private final String[] parts;

    private static class BasicMqttTopicAddress extends MqttTopicAddress<BasicMqttTopicAddress>
    {
        protected BasicMqttTopicAddress(String pattern)
        {
            super(pattern);
        }

        protected BasicMqttTopicAddress(String[] parts)
        {
            super(parts);
        }

        @Override protected BasicMqttTopicAddress create(String[] parts)
        {
            return new BasicMqttTopicAddress(parts);
        }
    }

    public static MqttTopicAddress pattern(String pattern)
    {
        return new BasicMqttTopicAddress(pattern);
    }

    protected MqttTopicAddress(String pattern)
    {
        parts = pattern.split("/");

        if (parts.length == 0)
            throw new IllegalArgumentException("Empty pattern");

        if (asList(parts).contains(""))
            throw new IllegalArgumentException("Missing name in pattern");

        if (asList(parts).subList(0, parts.length - 1).indexOf("#") > -1)
            throw new IllegalArgumentException("Terminal wildcard not terminal");
    }

    protected abstract T create(String[] parts);

    protected MqttTopicAddress(String[] parts)
    {
        this.parts = parts;
    }

    public Optional<T> match(String topicName)
    {
        final List<String> matched = new ArrayList<>(parts.length);
        final String[] names = topicName.split("/");
        int i = 0, j = 0;
        for ( ; i < parts.length && j < names.length; i++, j++)
        {
            if (parts[i].equals("#"))
            {
                matched.add(toString(copyOfRange(names, j, names.length, String[].class)));
                return Optional.of(create(matched.toArray(new String[0])));
            }
            else if (parts[i].equals("+"))
            {
                matched.add(names[j]);
            }
            else if (!parts[i].equals(names[j]))
            {
                return Optional.empty();
            }
        }
        return i == j && j == names.length ? Optional.of(create(matched.toArray(new String[0]))) : Optional.empty();
    }

    public T substitute(String... substitutions)
    {
        final String[] names = Arrays.copyOf(parts, parts.length);
        for (int i = 0, j = 0; i < parts.length && j < substitutions.length; i++)
        {
            if (parts[i].equals("#") || parts[i].equals("+"))
                names[i] = substitutions[j++];
        }
        return create(names);
    }

    public T substitute(int index, String name)
    {
        final String[] names = Arrays.copyOf(parts, parts.length);
        names[index] = name;
        return create(names);
    }

    @Override public int size()
    {
        return parts.length;
    }

    @Override public String get(int index)
    {
        return parts[index];
    }

    @Override public String toString()
    {
        return toString(parts);
    }

    private static String toString(String[] parts)
    {
        return join("/", parts);
    }
}
