package io.viktoriadb.util;

@FunctionalInterface
public interface TriConsumerInt<T1, T2> {
    void accept(T1 t1, T2 t2, int t3);
}
