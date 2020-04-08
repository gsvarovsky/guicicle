/*
 * Copyright (c) George Svarovsky 2020. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.m_ld.guicicle.mqtt.MqttTopicAddress.pattern;

public class MqttTopicAddressTest
{
    @Test(expected = IllegalArgumentException.class)
    public void testEmpty()
    {
        pattern("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonTerminalTerminal()
    {
        pattern("any/#/thing");
    }

    @Test
    public void testSubTreeMatch()
    {
        assertEquals(pattern("anything"), pattern("#").match("anything").orElseThrow(AssertionError::new));
        assertEquals(pattern("any/thing"), pattern("any/#").match("any/thing").orElseThrow(AssertionError::new));
        assertEquals(pattern("any/th/ing"), pattern("any/#").match("any/th/ing").orElseThrow(AssertionError::new));
        assertEquals(pattern("any/th/ing"), pattern("any/th/#").match("any/th/ing").orElseThrow(AssertionError::new));
    }

    @Test
    public void testSubTreeNoMatch()
    {
        assertFalse(pattern("a#").match("anything").isPresent());
        assertFalse(pattern("some/#").match("any/th/ing").isPresent());
        assertFalse(pattern("any/th#").match("any/th/ing").isPresent());
    }

    @Test
    public void testSingleLevelMatch()
    {
        assertEquals(pattern("any/thing"), pattern("any/+").match("any/thing").orElseThrow(AssertionError::new));
        assertEquals(pattern("any/th/ing"), pattern("any/th/+").match("any/th/ing").orElseThrow(AssertionError::new));
        assertEquals(pattern("any/th/ing"), pattern("any/+/ing").match("any/th/ing").orElseThrow(AssertionError::new));
        assertEquals(pattern("any/th/ing"), pattern("+/th/ing").match("any/th/ing").orElseThrow(AssertionError::new));
    }

    @Test
    public void testSingleLevelNoMatch()
    {
        assertFalse(pattern("a+").match("anything").isPresent());
        assertFalse(pattern("any/+").match("any/th/ing").isPresent());
        assertFalse(pattern("some/+/ing").match("any/th/ing").isPresent());
        assertFalse(pattern("any/th+/ing").match("any/th/ing").isPresent());
    }

    @Test
    public void testSubtitution()
    {
        assertEquals(pattern("any/thing"), pattern("any/+").substitute(1, "thing"));
        assertEquals(pattern("any/thing"), pattern("any/#").substitute(1, "thing"));
        assertEquals(pattern("any/th/ing"), pattern("any/+/ing").substitute(1, "th"));
        assertEquals(pattern("any/th/ing"), pattern("any/#").substitute(1, "th/ing"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonTerminalMultipleSubstitute()
    {
        pattern("any/+/foo").substitute(1, "th/ing");
    }
}