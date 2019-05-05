/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle;

import com.google.common.base.Objects;
import com.google.common.reflect.Reflection;

import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * Utility to provide runtime override of the methods of target objects, for use where the objects are created outside
 * of the current code's control, and a fully-implemented delegate class would be too onerous.
 */
public class PartialFluentProxy
{
    /**
     * Utility class to extend for readable delegate classes.
     *
     * @param <T> the target class
     */
    @SuppressWarnings("unused")
    public static class Delegate<T>
    {
        protected final T inner;
        protected T proxy;
        final Lookup lookup;

        public Delegate(T inner, Lookup lookup)
        {
            this.inner = inner;
            this.lookup = lookup.in(getClass());
        }
    }

    /**
     * Creates a factory function for proxies that implement the given target class and override methods in target
     * objects using the declared methods of the given delegate class.
     *
     * @param targetClass    the class to provide a delegating proxy for
     * @param delegateLookup a lookup into the delegate class with overriding declared methods
     * @param createDelegate a factory function for the delegate objects
     * @param <T>            the target class
     * @return a factory function for objects implementing the given target class, taking a raw target object
     */
    public static <T> UnaryOperator<T> factory(Class<T> targetClass,
                                               Lookup delegateLookup,
                                               Function<T, ?> createDelegate)
    {
        Map<MethodKey, Method> targetMethods = methodMap(targetClass.getMethods());
        Map<MethodKey, Method> delegateMethods = methodMap(delegateLookup.lookupClass().getDeclaredMethods());

        final Map<Method, Method> methodMap = targetMethods.entrySet().stream().filter(
            e -> delegateMethods.containsKey(e.getKey()))
            .collect(toMap(Map.Entry::getValue, e -> delegateMethods.get(e.getKey())));

        return target -> {
            final Object delegate = createDelegate.apply(target);
            //noinspection UnstableApiUsage
            return Reflection.newProxy(targetClass, (proxy, method, args) -> {
                final Method delegateMethod = methodMap.get(method);
                final Object rtn;
                if (delegateMethod != null)
                {
                    if (delegate instanceof Delegate)
                        ((Delegate)delegate).proxy = proxy;
                    rtn = delegateLookup.unreflect(delegateMethod).bindTo(delegate).invokeWithArguments(args);
                }
                else
                {
                    rtn = method.invoke(target, args);
                }
                // Maintain fluent chaining
                return rtn != null && targetClass.isAssignableFrom(rtn.getClass()) ? proxy : rtn;
            });
        };
    }

    /**
     * Create a proxy that implements the given target class and overrides methods in the given target object using the
     * declared methods of the given delegate.
     * <p>
     * This method introspects the given class and delegate object at call-time, so may not be suitable for a
     * high-throughput environment. To perform the introspection in advance of proxy creation, use {@link
     * #factory(Class, Lookup, Function)}.
     *
     * @param targetClass the class to provide a delegating proxy for
     * @param target      the target object with default method implementations
     * @param delegate    the delegate object with overriding declared methods
     * @param <T>         the target class
     * @return an object implementing the given target class
     */
    public static <T> T create(Class<T> targetClass, T target, Delegate<? extends T> delegate)
    {
        return factory(targetClass, delegate.lookup, t -> delegate).apply(target);
    }

    private static class MethodKey
    {
        final String name;
        final List<Class<?>> params;

        MethodKey(String name, Class<?>... params)
        {
            this.name = name;
            this.params = stream(params).collect(toList());
        }

        @Override public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MethodKey methodKey = (MethodKey)o;
            return Objects.equal(name, methodKey.name) &&
                Objects.equal(params, methodKey.params);
        }

        @Override public int hashCode()
        {
            return Objects.hashCode(name, params);
        }
    }

    private static Map<MethodKey, Method> methodMap(Method[] methods)
    {
        return stream(methods).collect(
            toMap(m -> new MethodKey(m.getName(), m.getParameterTypes()), m -> m,
                  // Choose the more specific return type, as per Class#getMethod
                  (m1, m2) -> m2.getReturnType().isAssignableFrom(m1.getReturnType()) ? m1 : m2));
    }
}
