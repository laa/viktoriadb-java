package io.viktoriadb.util;

public final class ObjectObjectIntTriple<T1, T2> implements Triple<T1, T2, Integer> {
    public final T1 first;
    public final T2 second;
    public final int third;

    public ObjectObjectIntTriple(T1 first, T2 second, int third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    @Override
    public T1 first() {
        return first;
    }

    @Override
    public T2 second() {
        return second;
    }

    @Override
    public Integer third() {
        return third;
    }
}
