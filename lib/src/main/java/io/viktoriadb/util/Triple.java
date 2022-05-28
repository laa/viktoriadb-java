package io.viktoriadb.util;

interface Triple<T1, T2, T3> {
    T1 first();

    T2 second();

    T3 third();
}
