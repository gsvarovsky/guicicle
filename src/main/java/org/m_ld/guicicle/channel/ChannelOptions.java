/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;

import static java.util.Objects.requireNonNull;

public class ChannelOptions extends DeliveryOptions
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

    public ChannelOptions()
    {
    }

    public ChannelOptions(ChannelOptions other)
    {
        super(other);
        this.delivery = other.delivery;
        this.quality = other.quality;
    }

    public ChannelOptions(JsonObject json)
    {
        super(json);
        if (json.containsKey("delivery"))
            this.delivery = Delivery.valueOf(json.getString("delivery"));
        if (json.containsKey("quality"))
            this.quality = Quality.valueOf(json.getString("quality"));
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

    @Override public JsonObject toJson()
    {
        final JsonObject json = super.toJson();
        json.put("delivery", delivery.name());
        json.put("quality", quality.name());
        return json;
    }
}
