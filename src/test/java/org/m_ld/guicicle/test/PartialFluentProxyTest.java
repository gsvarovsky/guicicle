/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.test;

import org.junit.Test;
import org.m_ld.guicicle.PartialFluentProxy;
import org.m_ld.guicicle.PartialFluentProxy.Delegate;

import static java.lang.invoke.MethodHandles.lookup;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * This class is in a different package to its test class, to verify object member access
 */
public class PartialFluentProxyTest
{
    public interface Target
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
        Target proxy = PartialFluentProxy.factory(Target.class, lookup().in(PartialDelegate.class), PartialDelegate::new)
            .apply(new TargetImpl());
        assertEquals("Hello", proxy.notDelegated());
    }

    @Test
    public void testDelegated()
    {
        Target proxy = PartialFluentProxy.factory(Target.class, lookup().in(PartialDelegate.class), PartialDelegate::new)
            .apply(new TargetImpl());
        assertEquals(2, proxy.delegated());
    }

    @Test
    public void testFluentNotDelegated()
    {
        Target proxy = PartialFluentProxy.factory(Target.class, lookup().in(PartialDelegate.class), PartialDelegate::new)
            .apply(new TargetImpl());
        assertEquals(2, proxy.fluentNotDelegated("Goodbye").delegated());
        assertEquals("Goodbye", proxy.fluentNotDelegated("Goodbye").notDelegated());
    }

    @Test
    public void testFluentDelegated()
    {
        Target proxy = PartialFluentProxy.factory(Target.class, lookup().in(PartialDelegate.class), PartialDelegate::new)
            .apply(new TargetImpl());
        assertEquals(2, proxy.fluentDelegated("Goodbye").delegated());
        assertEquals("Override!", proxy.fluentDelegated("Goodbye").notDelegated());
    }

    @Test
    public void testCreateWithAnonymousDelegate()
    {
        final TargetImpl target = new TargetImpl();
        //noinspection unused
        Target proxy = PartialFluentProxy.create(
            Target.class, target, new Delegate<Target>(target, lookup())
            {
                int delegated()
                {
                    assertNotNull(proxy);
                    return 2;
                }

                Target fluentDelegated(String value)
                {
                    return inner.fluentDelegated("Override!");
                }
            });
        assertEquals(2, proxy.delegated());
        assertEquals("Override!", proxy.fluentDelegated("Goodbye").notDelegated());
    }
}