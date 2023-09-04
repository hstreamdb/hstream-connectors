package io.hstream.io.standalone;

import io.hstream.io.KvStore;

import java.util.concurrent.CompletableFuture;

public class NullKvStore implements KvStore {
    @Override
    public CompletableFuture<Void> set(String key, String val) {
        throw new RuntimeException("NULL KV STORE");
    }

    @Override
    public CompletableFuture<String> get(String key) {
        throw new RuntimeException("NULL KV STORE");
    }

    @Override
    public void close() throws InterruptedException {
        throw new RuntimeException("NULL KV STORE");
    }
}
