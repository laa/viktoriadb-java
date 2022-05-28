package io.viktoriadb;

import java.time.Duration;

public record Options(Duration timeout, boolean readOnly, long initialMMapSize) {
}
