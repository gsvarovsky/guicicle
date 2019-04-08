/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.jackson;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;

import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class VertxJacksonModule extends AbstractModule
{
    @Override protected void configure()
    {
        newSetBinder(binder(), Module.class);
    }

    @Provides ObjectMapper objectMapper(Set<Module> jacksonModules)
    {
        final ObjectMapper objectMapper = new ObjectMapper();
        jacksonModules.forEach(objectMapper::registerModule);
        return objectMapper;
    }
}
