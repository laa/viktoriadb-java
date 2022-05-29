package io.viktoriadb;

public record Options(boolean noSync, boolean readOnly, long initialSize) {
}
