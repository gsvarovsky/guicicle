/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.m_ld.guicicle.mqtt.MqttUtil.isMatch;

public class MqttUtilTest
{
    @Test
    public void testSubTreeMatch()
    {
        assertTrue(isMatch("anything", "#"));
        assertFalse(isMatch("anything", "a#"));
        assertTrue(isMatch("any/thing", "any/#"));
        assertTrue(isMatch("any/th/ing", "any/#"));
        assertTrue(isMatch("any/th/ing", "any/th/#"));
        assertFalse(isMatch("any/th/ing", "some/#"));
        assertFalse(isMatch("any/th/ing", "any/th#"));
    }

    @Test
    public void testSingleLevelMatch()
    {
        assertTrue(isMatch("anything", "+"));
        assertFalse(isMatch("anything", "a+"));
        assertTrue(isMatch("any/thing", "any/+"));
        assertFalse(isMatch("any/th/ing", "any/+"));
        assertTrue(isMatch("any/th/ing", "any/th/+"));
        assertFalse(isMatch("any/th/ing", "some/+/ing"));
        assertFalse(isMatch("any/th/ing", "any/th+/ing"));
        assertTrue(isMatch("any/th/ing", "any/+/ing"));
        assertTrue(isMatch("any/th/ing", "+/th/ing"));
    }
}