package com.github.basking2.sdsai.dsds.node;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.function.Function;

public class NodeUtil {

    public static <USERKEY, STOREKEY> void storeNode(
            final OutputStream out,
            final Node<USERKEY, STOREKEY> node,
            final Function<USERKEY, byte[]> storeUserKey,
            final Function<STOREKEY, byte[]> storeStoreKey
    ) throws IOException {
        final byte[] intHeader = new byte[4 * 6];
        final ByteBuffer bb = ByteBuffer.wrap(intHeader);
        bb.putInt(node.getChildCap());
        bb.putInt(node.getDataCap());
        bb.putInt(node.getAncestorsCap());

        bb.putInt(node.getChildren().size());
        bb.putInt(node.getData().size());
        bb.putInt(node.getAncestors().size());

        out.write(intHeader);

        for (final STOREKEY k : node.getChildren()) {
            writeLengthValueArray(out, storeStoreKey.apply(k));
        }
        for (final USERKEY k : node.getData()) {
            writeLengthValueArray(out, storeUserKey.apply(k));
        }
        for (final STOREKEY k : node.getAncestors()) {
            writeLengthValueArray(out, storeStoreKey.apply(k));
        }
    }

    /**
     * Load a node from an input stream.
     *
     * @param in The input stream to read from.
     * @param loadUserKey How to convert bytes to a key for the user.
     * @param loadStoreKey How to convert bytes to a key that the storage system uses.
     * @param <USERKEY> The type of the user key.
     * @param <STOREKEY> The type of the storage key.
     * @return The unmarshalled node.
     * @throws IOException On any error.
     */
    public static <USERKEY, STOREKEY> Node<USERKEY, STOREKEY> readNode(
            final InputStream in,
            final Function<byte[], USERKEY> loadUserKey,
            final Function<byte[], STOREKEY> loadStoreKey
    ) throws IOException {
        final byte[] threeNodeInts = new byte[4 * 6];
        mustRead(in, threeNodeInts, 0, threeNodeInts.length);
        final ByteBuffer bb = ByteBuffer.wrap(threeNodeInts);

        final int childCap = bb.getInt();
        final int dataCap = bb.getInt();
        final int ancestorsCap = bb.getInt();
        final Node<USERKEY, STOREKEY> node = new Node(childCap, dataCap, ancestorsCap);

        final int children = bb.getInt();
        final int datums = bb.getInt();
        final int ancestors = bb.getInt();

        for (int i = 0; i < children; i++) {
            node.getChildren().add(loadStoreKey.apply(mustReadLengthValueArray(in)));
        }

        for (int i = 0; i < datums; i++) {
            node.getData().add(loadUserKey.apply(mustReadLengthValueArray(in)));
        }

        for (int i = 0; i < ancestors; i++) {
            node.getAncestors().add(loadStoreKey.apply(mustReadLengthValueArray(in)));
        }

        return node;
    }

    /**
     * A function what will read from the given {@link InputStream} into the given buffer of throw an IOException.
     *
     * This could be moved to a utility class at a later time.
     *
     * This replicates ideas in many IOUtil classes.
     *
     * @param in From where to read data.
     * @param buffer The buffer to write in to.
     * @param offset The offset into the buffer.
     * @param requiredLength The required length.
     * @throws IOException On an error, including a shortened read.
     */
    private static void mustRead(final InputStream in, final byte[] buffer, final int offset, final int requiredLength) throws IOException
    {
        int len = 0;
        while (len < requiredLength) {
            final int read = in.read(buffer, offset + len, requiredLength - len);

            if (read < 0) {
                throw new IOException("The input stream is closed before we read "+requiredLength+" bytes.");
            }

            len += read;
        }
    }

    /**
     * Read a 4 byte int and then that many bytes following. The second array of bytes is returned.
     * @param in The input stream.
     * @return The second read of the length.
     * @throws IOException On any error.
     */
    private static byte[] mustReadLengthValueArray(final InputStream in) throws IOException {
        final byte[] lenBuf = new byte[4];
        mustRead(in, lenBuf, 0, lenBuf.length);
        final int len = ByteBuffer.wrap(lenBuf).getInt();
        if (len < 0) {
            throw new IOException("Encoded length was negative: "+len);
        }
        final byte[] data = new byte[len];
        mustRead(in, data, 0, len);
        return data;
    }

    /**
     * Write an integer that is the length of the given array followed by the contents of the array.
     *
     * This is the reverse of {@link #mustReadLengthValueArray(InputStream)}.
     *
     * @param data The data to write, prefixed with its length as an integer.
     * @throws IOException On any error.
     */
    private static void writeLengthValueArray(final OutputStream out, final byte[] data) throws IOException {
        final byte[] num = new byte[4];
        ByteBuffer.wrap(num).putInt(data.length);
        out.write(num);
        out.write(data);
    }
}
