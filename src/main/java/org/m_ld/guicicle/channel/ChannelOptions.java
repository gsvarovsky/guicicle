/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import org.m_ld.guicicle.http.ResponseStatusMapper;

import java.util.HashMap;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class ChannelOptions extends DeliveryOptions implements ResponseStatusMapper
{
    public enum Delivery
    {
        SEND, PUBLISH
    }

    public enum Quality
    {
        AT_MOST_ONCE, AT_LEAST_ONCE, EXACTLY_ONCE
    }

    private Delivery delivery = Delivery.PUBLISH;
    private Quality quality = Quality.AT_MOST_ONCE;
    private Map<String, HttpResponseStatus> statuses = new HashMap<>();
    private boolean echo = false;

    public ChannelOptions()
    {
    }

    public ChannelOptions(ChannelOptions other)
    {
        super(other);
        this.delivery = other.delivery;
        this.quality = other.quality;
        this.statuses.putAll(other.statuses);
        this.echo = other.echo;
    }

    public ChannelOptions(JsonObject json)
    {
        super(json);
        if (json.containsKey("delivery"))
            this.delivery = Delivery.valueOf(json.getString("delivery"));
        if (json.containsKey("quality"))
            this.quality = Quality.valueOf(json.getString("quality"));
        if (json.containsKey("statuses"))
            this.statuses.putAll(json.getJsonObject("statuses").stream().collect(
                toMap(Map.Entry::getKey, e -> HttpResponseStatus.parseLine(e.getValue().toString()))));
        if (json.containsKey("echo"))
            this.echo = json.getBoolean("echo");
    }

    public Delivery getDelivery()
    {
        return delivery;
    }

    public ChannelOptions setDelivery(Delivery delivery)
    {
        this.delivery = requireNonNull(delivery, "Delivery type must not be null");
        return this;
    }

    public Quality getQuality()
    {
        return quality;
    }

    public ChannelOptions setQuality(Quality quality)
    {
        this.quality = requireNonNull(quality, "Quality of service must not be null");
        return this;
    }

    public HttpResponseStatus getStatusForError(Throwable error)
    {
        return statuses.getOrDefault(error.getClass().getSimpleName(), INTERNAL_SERVER_ERROR);
    }

    public ChannelOptions setStatusForError(Map<String, HttpResponseStatus> statusForError)
    {
        statuses.clear();
        statuses.putAll(statusForError);
        return this;
    }

    public boolean isEcho()
    {
        return echo;
    }

    public ChannelOptions setEcho(boolean echo)
    {
        this.echo = echo;
        return this;
    }

    @Override public ChannelOptions setSendTimeout(long timeout)
    {
        super.setSendTimeout(timeout);
        return this;
    }

    @Override public ChannelOptions setCodecName(String codecName)
    {
        super.setCodecName(codecName);
        return this;
    }

    @Override public ChannelOptions setHeaders(MultiMap headers)
    {
        super.setHeaders(headers);
        return this;
    }

    @Override public ChannelOptions setLocalOnly(boolean localOnly)
    {
        super.setLocalOnly(localOnly);
        return this;
    }

    @Override public ChannelOptions addHeader(String key, String value)
    {
        super.addHeader(key, value);
        return this;
    }

    public ChannelOptions setOptions(DeliveryOptions options)
    {
        setCodecName(options.getCodecName());
        setHeaders(options.getHeaders());
        setSendTimeout(options.getSendTimeout());
        setLocalOnly(options.isLocalOnly());
        if (options instanceof ChannelOptions)
        {
            ChannelOptions channelOptions = (ChannelOptions)options;
            setDelivery(channelOptions.getDelivery());
            setQuality(channelOptions.getQuality());
            setStatusForError(channelOptions.statuses);
            setEcho(channelOptions.isEcho());
        }
        return this;
    }

    @Override public JsonObject toJson()
    {
        final JsonObject json = super.toJson();
        json.put("delivery", delivery.name());
        json.put("quality", quality.name());
        if (!statuses.isEmpty())
            json.put("statuses", new JsonObject(statuses.entrySet().stream().collect(
                toMap(Map.Entry::getKey, e -> e.getValue().toString()))));
        json.put("echo", echo);
        return json;
    }
}
