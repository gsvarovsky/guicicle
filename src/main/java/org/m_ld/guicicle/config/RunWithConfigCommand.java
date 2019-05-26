/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.config;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.cli.annotations.Name;
import io.vertx.core.cli.annotations.Summary;
import io.vertx.core.impl.launcher.commands.RunCommand;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.vertx.core.Future.succeededFuture;
import static java.util.Collections.singletonList;

@Name("run") // Overrides RunCommand
@Summary("Runs a verticle called <main-verticle> in its own instance of vert.x using config from vertx-config.")
public class RunWithConfigCommand extends RunCommand
{
    @Override public synchronized void deploy(String verticle, Vertx vertx, DeploymentOptions options,
                                              Handler<AsyncResult<String>> completionHandler)
    {
        retrieveConfig(vertx, options.getConfig(), configResult -> {
            if (configResult.failed())
            {
                completionHandler.handle(configResult.mapEmpty());
            }
            else
            {
                final DeploymentOptions deploymentOptions = options.setConfig(configResult.result());
                super.beforeDeployingVerticle(deploymentOptions);
                super.deploy(verticle, vertx, deploymentOptions, completionHandler);
            }
        });
    }

    @Override protected void beforeDeployingVerticle(DeploymentOptions deploymentOptions)
    {
        // Do nothing here, we are in the run() method - we will call super after getting the config
    }

    private void retrieveConfig(Vertx vertx, JsonObject config, Handler<AsyncResult<JsonObject>> retrievedHandler)
    {
        if (config.containsKey("config"))
        {
            final ConfigRetrieverOptions configOptions =
                new ConfigRetrieverOptions(config.getJsonObject("config"));
            config.remove("config"); // So we don't recurse indefinitely

            // We're not in a verticle, so the initial default store is the passed-in config
            final List<ConfigStoreOptions> stores = new ArrayList<>(
                singletonList(new ConfigStoreOptions().setType("json").setConfig(config)));
            if (configOptions.isIncludeDefaultStores())
            {
                configOptions.setIncludeDefaultStores(false);
                stores.add(new ConfigStoreOptions().setType("sys"));
                stores.add(new ConfigStoreOptions().setType("env"));
                getDefaultConfigStore().ifPresent(stores::add);
            }
            stores.addAll(configOptions.getStores());
            configOptions.setStores(stores);

            ConfigRetriever.create(vertx, configOptions).getConfig(configResult -> {
                if (configResult.succeeded())
                    retrieveConfig(vertx, configResult.result(), retrievedHandler);
                else
                    retrievedHandler.handle(configResult.mapEmpty());
            });
        }
        else
        {
            retrievedHandler.handle(succeededFuture(config));
        }
    }

    private Optional<ConfigStoreOptions> getDefaultConfigStore()
    {
        // TODO: This is quite involved, and buried in ConfigRetrieverImpl
        return Optional.empty();
    }
}
