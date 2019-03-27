![stability-wip](https://img.shields.io/badge/stability-work_in_progress-lightgrey.svg)

# guicicle
_super-simple Vert.x Guice integration and utilities_

1. Extend [Guicicle](src/main/java/org/m_ld/guicicle/Guicicle.java)
1. `@Inject` fields of your verticle (not the constructor, as injection happens during Verticle `init()`; although injected classes can themselves use constructor injection happily)
1. Provide Guice module class names in Verticle config, either in `guice.modules` (as an array) or `guice.module` (as a string)

The injector available to the modules includes:
* The `Vertx` instance
* Vert.x config elements as plain Java objects (Maps, Lists, Strings and Numbers), using dot notation
* An `Executor` for blocking code


