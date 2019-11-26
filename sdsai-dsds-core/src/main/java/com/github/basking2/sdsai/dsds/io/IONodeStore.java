package com.github.basking2.sdsai.dsds.io;

import com.github.basking2.sdsai.dsds.node.Node;
import com.github.basking2.sdsai.dsds.node.NodeStore;
import com.github.basking2.sdsai.dsds.node.NodeStoreException;
import com.github.basking2.sdsai.dsds.node.NodeUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;


/**
 * An implementation of {@link NodeStore} that stores and returns streams of data.
 *
 * @param <USERKEY> The user key.
 *
 */
public class IONodeStore<USERKEY> implements NodeStore<USERKEY, byte[], InputStream> {

    private final NodeAccessor<USERKEY> accessor;

    public IONodeStore(
            final NodeAccessor<USERKEY> accessor
    )
    {
        this.accessor = accessor;
    }

    @Override
    public InputStream loadData(final byte[] key) {
        try (final InputStream in = accessor.reader(key)) {
            return in;
        }
        catch (final IOException e) {
            throw new NodeStoreException("Opening "+key, e);
        }
    }

    @Override
    public Node<USERKEY, byte[]> loadNode(final byte[] key) {
        try (final InputStream in = accessor.reader(key)) {

            return NodeUtil.readNode(
                    in,
                    userKey -> accessor.loadUserKey(userKey),
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
        try (final OutputStream out = accessor.writer(key)) {
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
        try (final OutputStream out = accessor.writer(key)) {
            NodeUtil.storeNode(out, node, k -> convert(k), k -> k);
        }
        catch (final IOException e) {
            throw new NodeStoreException(e.getMessage(), e);
        }
    }

    @Override
    public void removeNode(final byte[] key) {
        accessor.delete(key);
    }

    @Override
    public void removeData(final byte[] key) {
        accessor.delete(key);
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
        return accessor.storeUserKey(key);
    }

    /**
     * How a {@link IONodeStore} accesses {@link Node} data.
     */
    public interface NodeAccessor<USERKEY> {

        InputStream reader(byte[] key);
        OutputStream writer(byte[] key);
        void delete(byte[] key);

        USERKEY loadUserKey(byte[] key);
        byte[] storeUserKey(USERKEY key);
    }
}
