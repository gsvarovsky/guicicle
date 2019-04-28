/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PartialFluentProxyTest
{
    interface Target
    {
        int delegated();
        String notDelegated();
        Target fluentDelegated(String value);
        Target fluentNotDelegated(String value);
    }

    class TargetImpl implements Target
    {
        String value = "Hello";

        @Override public int delegated()
        {
            return 1;
        }

        @Override public String notDelegated()
        {
            return value;
        }

        @Override public Target fluentDelegated(String value)
        {
            this.value = value;
            return this;
        }

        @Override public Target fluentNotDelegated(String value)
        {
            this.value = value;
            return this;
        }
    }

    @SuppressWarnings("unused") static class PartialDelegate
    {
        final Target target;

        PartialDelegate(Target target)
        {
            this.target = target;
        }

        int delegated()
        {
            return 2;
        }

        Target fluentDelegated(String value)
        {
            return target.fluentDelegated("Override!");
        }
    }

    @Test
    public void testNotDelegated()
    {
        Target delegate = PartialFluentProxy.factory(Target.class, PartialDelegate.class, PartialDelegate::new)
            .apply(new TargetImpl());
        assertEquals("Hello", delegate.notDelegated());
    }

    @Test
    public void testDelegated()
    {
        Target delegate = PartialFluentProxy.factory(Target.class, PartialDelegate.class, PartialDelegate::new)
            .apply(new TargetImpl());
        assertEquals(2, delegate.delegated());
    }

    @Test
    public void testFluentNotDelegated()
    {
        Target delegate = PartialFluentProxy.factory(Target.class, PartialDelegate.class, PartialDelegate::new)
            .apply(new TargetImpl());
        assertEquals(2, delegate.fluentNotDelegated("Goodbye").delegated());
        assertEquals("Goodbye", delegate.fluentNotDelegated("Goodbye").notDelegated());
    }

    @Test
    public void testFluentDelegated()
    {
        Target delegate = PartialFluentProxy.factory(Target.class, PartialDelegate.class, PartialDelegate::new)
            .apply(new TargetImpl());
        assertEquals(2, delegate.fluentDelegated("Goodbye").delegated());
        assertEquals("Override!", delegate.fluentDelegated("Goodbye").notDelegated());
    }

    @Test
    public void testCreateWithAnonymousDelegate()
    {
        final TargetImpl target = new TargetImpl();
        //noinspection unused
        Target delegate = PartialFluentProxy.create(Target.class, target, new Object()
        {
            int delegated()
            {
                return 2;
            }

            Target fluentDelegated(String value)
            {
                return target.fluentDelegated("Override!");
            }
        });
        assertEquals(2, delegate.delegated());
        assertEquals("Override!", delegate.fluentDelegated("Goodbye").notDelegated());
    }
}