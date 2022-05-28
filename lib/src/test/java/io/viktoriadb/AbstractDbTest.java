package io.viktoriadb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Random;
import java.util.function.Consumer;

public class AbstractDbTest {
    public void runTest(Consumer<DB> consumer) {
        Path tempFile;
        try {
            tempFile = Files.createTempFile("vdb", "data");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (var db = DB.open(tempFile, null)) {
            consumer.accept(db);
            db.executeInsideWriteTx(Tx::check);
        }
        try {
            Files.delete(tempFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ByteBuffer bytes(String string) {
        var bytes = string.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.wrap(bytes);
    }

    public static String string(ByteBuffer buffer) {
        var bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public ArrayList<ByteBuffer[]> randomData() {
        int count = 1_000;
        ArrayList<ByteBuffer[]> data = new ArrayList<>();
        Random rnd = new Random(42);

        for (int i = 0; i < count; i++) {
            int keySize = rnd.nextInt(20) + 16;
            int valueSize = rnd.nextInt(1000);

            var key = new byte[keySize];
            var value = new byte[valueSize];

            rnd.nextBytes(key);
            rnd.nextBytes(value);

            var kv = new ByteBuffer[]{ByteBuffer.wrap(key), ByteBuffer.wrap(value)};
            data.add(kv);
        }

        return data;
    }
}
