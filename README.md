# Codecrafters redis challenge in Rust
Building my own redis (really a tcp server so far) in order to learn the details of Rust's 
Async Ecosystem (and hopefully how to use unsafe to build my own datastructures in Rust).

This means that I will try to use as little async libraries as I can (but still leveraging the `futures` crate).
I built a simple executor and reactor myself (based on many references).

Eventually I will move to using smol and it's ecosystem of crates (for testing the redis impl at scale).

Also want to play with OpenTelemetry for tracking requests (+ metrics) throughout a distributed system.
# References
- https://github.com/smol-rs
- https://build-your-own.org/redis
- https://redis.io/docs/latest/develop/reference/protocol-spec/
- https://github.com/codecrafters-io/redis-tester
- https://rust-lang.github.io/async-book/
- https://www.packtpub.com/en-us/product/asynchronous-programming-in-rust-9781805128137

