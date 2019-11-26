package com.github.basking2.sdsai.dsds.io;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Consumer;
import java.util.function.Function;

public class IONodeStoreFactory {
    public static IONodeStore<String> buildStringKeyStore(
            final Function<byte[], InputStream> reader,
            final Function<byte[], OutputStream> writer,
            final Consumer<byte[]> delete
    ) {
        return new IONodeStore<String>(
                new IONodeStore.NodeAccessor<String>() {
                    @Override
                    public InputStream reader(final byte[] key) {
                        return reader.apply(key);
                    }

                    @Override
                    public OutputStream writer(final byte[] key) {
                        return writer.apply(key);
                    }

                    @Override
                    public void delete(final byte[] key) {
                        delete.accept(key);
                    }

                    @Override
                    public String loadUserKey(final byte[] key) {
                        return new String(key);
                    }

                    @Override
                    public byte[] storeUserKey(final String key) {
                        return key.getBytes();
                    }
                }
        );

    }
}
