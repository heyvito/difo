![Difo](https://user-images.githubusercontent.com/77198/85149541-cbbf4500-b227-11ea-9c93-ecf773516939.png)

![](https://github.com/heyvito/difo/workflows/Test/badge.svg)
[![cljdoc badge](https://cljdoc.org/badge/difo/difo)](https://cljdoc.org/d/difo/difo/CURRENT)
[![Clojars Project](https://img.shields.io/clojars/v/difo.svg)](https://clojars.org/difo)

**Difo** implements distributed work queues for background processing.<br/>
Think of it as a background task runner heavily inspired by [Sidekiq](https://github.com/mperham/sidekiq/) and [Resque](https://github.com/github/resque).

## Installation

Add the following dependency to your `project.clj` file:

```clojure
[difo "0.1.0"]
```

## Documentation

Documentation is available on [cljdoc](https://cljdoc.org/d/difo/difo/CURRENT)

## Usage

Difo uses [Redis](https://redis.io) as its storage backend, so a Redis instance is required in order to use it.
The library is divided into two parts: Client, and Server. Both implementations are present, so only one dependency is required.

### Server
A Difo process is responsible for spinning up an arbitrary, user-defined number of *Workers*. Each Worker has its own thread, and their function is to wait for *Unit*s to be processed. Each Unit represents a single task to be performed. Each process, by its turn, can watch a list of *Queues*.

#### Configuration
Server configuration is done through environment variables. The server process looks for three diferent variables, those being:

| Key         | Description |
|-------------|-------------|
| `WORKERS`   | An integer representing how many Workers will be started for the server process. Defaults to `10`. |
| `QUEUES`    | A list of queues to watch, separated by spaces. Defaults to `default`. |
| `REDIS_URL` | The URL to the Redis server, following the following format: `redis://[[username:]password@]hostname[:port][/database]`. Defaults to `redis://localhost:6379/0`. |

#### Running

In order to start a Difo server, your application must implement a separate entrypoint responsible for running it. In this entrypoint, one may also want to perform any other configuration required to initialize other libraries or mechanisms in order to process units:

```clojure
(ns myapplication.core
    (:require [difo.server :as difo-server]))

(defn -main-difo []
  (configure-my-application)
  (difo-server/start))
```

`difo.server/start` will block the calling thread until the server terminates.

### Client
In order to enqueue units, Difo provides a `difo.client` namespace. To use it, the following environment variables must be configured:

| Key         | Description |
|-------------|-------------|
| `REDIS_URL` | The URL to the Redis server, following the following format: `redis://[[username:]password@]hostname[:port][/database]`. Defaults to `redis://localhost:6379/0`. |

`REDIS_URL` **must the the same** as used by the server process.

Then, to enqueue an unit:

```clojure
(ns myapplication.sample
    (:require [difo.client :as background]))
    
(defn very-expensive-calculation [a b]
  (Thread/sleep 5000)
  (+ a b))
  
(defn do-something []
  (background/perform very-expensive-calculation 1 2))
```

This way, invoking `(do-something)` will cause a new unit to be enqueued for later processing. The client is responsible for serialising all parameters into an [EDN](https://github.com/edn-format/edn) structure, and enqueue it into a queue. By default, `perform` enqueues units to the `default` queue. To specify which queue to enqueue into, use `perform-on`:

```clojure
(ns myapplication.sample
    (:require [difo.client :as background]))
    
(defn very-expensive-calculation [a b]
  (Thread/sleep 5000)
  (+ a b))
  
(defn do-something []
  (background/perform-on :calculations very-expensive-calculation 1 2))
```

> :warning: **WARNING:** As mentioned, the client side of Difo will serialise arguments to your function. **This means the arguments to your workers must be simple datatypes**, such as numbers, strings, booleans, lists, vectors, and maps (hashes). Complex objects (like Atoms, Refs, DateTime...) will not serialise properly, and **will cause problems.**

## Details to keep in mind when using Difo

### 1. Keep parameters small and simple
Difo uses Redis to persist information about units to be processed. In order to do so, EDN is used as the serialisation format.
Complex objects cannot be serialised using EDN. For instance, attempting to serialise an `atom` will result in the following output:

```
#object[clojure.lang.Atom 0x6548bb7d {:status :ready, :val {:foo :bar}}]
```

Putting in simpler terms, **do not store state into Units**. In case one needs to provide a database row to a function that will be run in background, provide to it an identifier instead, and look it up within that function.

Arguments passed to either `perform` or `perform-on` must be simple types, such as `String`, `Long`, `Double`, `Boolean`, `nil`, `Keyword`, `Vector`, `List`, and `Map`. Difo uses `prn-str` and `clojure.edn/read-string` to serialise/deserialise data, and complex types (such as Atoms, for instance) will not survive the serialize/deserialize round trip.

### 2. Ensure your functions are idempotent and transactional
[Idempotency](https://en.wikipedia.org/wiki/Idempotence) means that a function enqueued for background processing can safely be executed multiple times. This will be specially important when automatic retries are implemented into Difo. When such feature lands, a unit can be half-executed, and then executed again, until it succeeds. That said, it is important to wrap operations in transactions, and make sure sensitive operations can be rolled-back in case an error happens during runtime.
In the future, Difo will ensure units will be executed **at least once**, but not **exactly** once.

## Contributions

...are more than welcome!
Feel free to fork and open a pull request. Please include tests, and ensure all tests are green!

## TODO
The following features will be implemented in the near future:

- [ ] Statistics
- [ ] Web Dashboard
- [ ] Automatic Retries
- [ ] Scheduled Units

## License

```
MIT License

Copyright (c) 2020 - Victor Gama de Oliveira

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
