package com.github.basking2.sdsai.dsds.node;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * An implementation of {@link NodeStore} that stores and returns streams of data.
 *
 * @param <USERKEY> The user key.
 *
 */
public class IONodeStore<USERKEY> implements NodeStore<USERKEY, byte[], InputStream> {

    private final Function<byte[], InputStream> reader;
    private final Function<byte[], OutputStream> writer;
    private final Consumer<byte[]> delete;
    private final Function<byte[], USERKEY> loadUserKey;
    private final Function<USERKEY, byte[]> storeUserKey;

    public IONodeStore(
            final Function<byte[], InputStream> reader,
            final Function<byte[], OutputStream> writer,
            final Consumer<byte[]> delete,
            final Function<byte[], USERKEY> loadUserKey,
            final Function<USERKEY, byte[]> storeUserKey
    )
    {
        this.reader = reader;
        this.writer = writer;
        this.delete = delete;
        this.loadUserKey = loadUserKey;
        this.storeUserKey = storeUserKey;
    }

    @Override
    public InputStream loadData(final byte[] key) {
        try (final InputStream in = reader.apply(key)) {
            return in;
        }
        catch (final IOException e) {
            throw new NodeStoreException("Opening "+key, e);
        }
    }

    @Override
    public Node<USERKEY, byte[]> loadNode(final byte[] key) {
        try (final InputStream in = reader.apply(key)) {

            return NodeUtil.readNode(
                    in,
                    userKey -> loadUserKey.apply(userKey),
                    storeKey -> storeKey
            );
        }
        catch (final IOException e) {
            throw new NodeStoreException("Opening "+key, e);
        }
    }

    @Override
    public void store(final byte[] key, final InputStream data) {
        final byte[] buffer = new byte[1024 * 4];
        try (final OutputStream out = writer.apply(key)) {
            while (true) {
                final int read = data.read(buffer);
                if (read < 0) {
                    return;
                }
                else {
                    out.write(buffer, 0, read);
                }
            }
        }
        catch (final IOException e) {
            throw new NodeStoreException(e.getMessage(), e);
        }
    }

    @Override
    public void store(final byte[] key, final Node<USERKEY, byte[]> node) {
        // The header of integer values.
        try (final OutputStream out = writer.apply(key)) {
            NodeUtil.storeNode(out, node, k -> convert(k), k -> k);
        }
        catch (final IOException e) {
            throw new NodeStoreException(e.getMessage(), e);
        }
    }

    @Override
    public void removeNode(final byte[] key) {
        delete.accept(key);
    }

    @Override
    public void removeData(final byte[] key) {
        delete.accept(key);
    }

    @Override
    public byte[] generateKey(final Node<USERKEY, byte[]> node, final InputStream inputStream) {
        if (node != null) {
            return ("node/" + UUID.randomUUID()).getBytes();
        }
        else {
            return ("data/" + UUID.randomUUID()).getBytes();
        }
    }

    /**
     * By default this returns the key parameter.
     *
     * @param key The user key. This is returned as the storage key.
     * @return Return a key for use in storing data.
     */
    @Override
    public byte[] convert(final USERKEY key) {
        return storeUserKey.apply(key);
    }
}
