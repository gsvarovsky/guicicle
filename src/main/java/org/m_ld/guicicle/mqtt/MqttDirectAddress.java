/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

public abstract class MqttDirectAddress<T extends MqttDirectAddress<T>> extends MqttTopicAddress<T>
{
    private MqttDirectAddress(String pattern)
    {
        super(pattern);
        if (!"+".equals(get(1)) || !"+".equals(get(2)) || !"+".equals(get(3)))
            throw new AssertionError("Expecting three wildcards");
    }

    public String toId()
    {
        return get(1);
    }

    public String fromId()
    {
        return get(2);
    }

    public String messageId()
    {
        return get(3);
    }

    public T toId(String toId)
    {
        return substitute(1, toId);
    }

    public T fromId(String fromId)
    {
        return substitute(2, fromId);
    }

    public T messageId(String messageId)
    {
        return substitute(3, messageId);
    }

    protected MqttDirectAddress(String[] parts)
    {
        super(parts);
    }

    public static class MqttSendAddress extends MqttDirectAddress<MqttSendAddress>
    {
        public static MqttSendAddress SEND_ADDRESS = new MqttSendAddress();

        private MqttSendAddress()
        {
            super("__send/+/+/+/#");
        }

        public String topic()
        {
            return get(4);
        }

        public MqttSendAddress topic(String topic)
        {
            return substitute(4, topic);
        }

        private MqttSendAddress(String[] parts)
        {
            super(parts);
        }

        @Override protected MqttSendAddress create(String[] parts)
        {
            return new MqttSendAddress(parts);
        }
    }

    public static class MqttReplyAddress extends MqttDirectAddress<MqttReplyAddress>
    {
        public static MqttReplyAddress REPLY_ADDRESS = new MqttReplyAddress();

        private MqttReplyAddress()
        {
            super("__reply/+/+/+");
        }

        private MqttReplyAddress(String[] parts)
        {
            super(parts);
        }

        @Override protected MqttReplyAddress create(String[] parts)
        {
            return new MqttReplyAddress(parts);
        }
    }
}