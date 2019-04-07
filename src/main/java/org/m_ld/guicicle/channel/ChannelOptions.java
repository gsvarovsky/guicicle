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
    public enum DeliveryType
    {
        P2P, PUB_SUB
    }

    private DeliveryType deliveryType = DeliveryType.PUB_SUB;

    public ChannelOptions()
    {
    }

    public ChannelOptions(ChannelOptions other)
    {
        super(other);
        this.deliveryType = other.deliveryType;
    }

    public ChannelOptions(JsonObject json)
    {
        super(json);
        this.deliveryType = DeliveryType.valueOf(json.getString("deliveryType"));
    }

    public DeliveryType getDeliveryType()
    {
        return deliveryType;
    }

    public ChannelOptions setDeliveryType(DeliveryType deliveryType)
    {
        this.deliveryType = requireNonNull(deliveryType, "Delivery type must not be null");
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
        json.put("deliveryType", deliveryType.name());
        return json;
    }
}
