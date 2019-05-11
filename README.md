[![Build Status](https://travis-ci.org/gsvarovsky/guicicle.svg?branch=master)](https://travis-ci.org/gsvarovsky/guicicle)
![stability-wip](https://img.shields.io/badge/stability-work_in_progress-lightgrey.svg)

# guicicle
_super-simple Vert.x Guice integration and utilities_

Provide Guice module class names in Verticle config, either in `guice.modules` (as an array) or `guice.module` (as a string). Then:

1. Extend [Guicicle](src/main/java/org/m_ld/guicicle/Guicicle.java).
1. `@Inject` fields of your verticle (not the constructor, as injection happens during Verticle `init()`; although injected classes can themselves use constructor injection happily).

OR

1. Write startable/stoppable services as [Vertice](src/main/java/org/m_ld/guicicle/Vertice.java)s, using any injection method.
1. Start a vanilla [Guicicle](src/main/java/org/m_ld/guicicle/Guicicle.java) and your bound Vertices will be constructed during `init()` and started when the Guicicle starts.

The injector available to the modules includes:
* The `Vertx` instance
* Vert.x config elements as plain Java objects (Maps, Lists, Strings and Numbers) and `JsonObject`s/`JsonArray`s, as applicable; using dot notation starting with `config`, e.g. `config.http.port`
* An `Executor` for blocking code


