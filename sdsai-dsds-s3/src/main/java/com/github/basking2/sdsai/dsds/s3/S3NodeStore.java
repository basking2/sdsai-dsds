package com.github.basking2.sdsai.dsds.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.github.basking2.sdsai.dsds.node.Node;
import com.github.basking2.sdsai.dsds.node.NodeStore;
import com.github.basking2.sdsai.dsds.node.NodeStoreException;
import com.github.basking2.sdsai.dsds.node.NodeUtil;

import java.io.*;
import java.util.UUID;

public class S3NodeStore implements NodeStore<String, S3ObjectId, InputStream> {
    private final AmazonS3 client;
    private final String prefix;
    private final String bucket;

    public S3NodeStore(final AmazonS3 client, final String bucket, final String prefix) {
        this.client = client;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    public S3NodeStore(final String bucket, final String prefix) {
        this(AmazonS3ClientBuilder.defaultClient(), bucket, prefix);
    }

    /**
     * The returned {@link S3Object} <b>must</b> be closed.
     *
     * @param key The key.
     * @return A freshly fetched {@link S3Object}.
     */
    @Override
    public InputStream loadData(final S3ObjectId key) {
        return client.getObject(new GetObjectRequest(key)).getObjectContent();
    }

    @Override
    public Node<String, S3ObjectId> loadNode(final S3ObjectId key) {
        try (final S3Object o = client.getObject(new GetObjectRequest(key))) {
            return NodeUtil.readNode(
                    o.getObjectContent(),
                    userKey -> new String(userKey),
                    storekey -> new S3ObjectId(bucket, new String(storekey))
            );
        }
        catch (final IOException e) {
            throw new NodeStoreException(e);
        }
    }

    @Override
    public void store(final S3ObjectId key, final InputStream data) {
        // NOTE: We do not set the content length, so S3 will copy the data into memory before sending
        //       so as to compute the content length.
        final ObjectMetadata objectMetadata = new ObjectMetadata();
        final PutObjectRequest putObjectRequest = new PutObjectRequest(key.getBucket(), key.getKey(), data, objectMetadata);
        client.putObject(putObjectRequest);
    }

    @Override
    public void store(final S3ObjectId key, final Node<String, S3ObjectId> node) {
        try {
            final ByteArrayOutputStream out = new ByteArrayOutputStream() {
                @Override
                public void close() throws IOException {
                    final ObjectMetadata objectMetadata = new ObjectMetadata();
                    objectMetadata.setContentLength(count);
                    final PutObjectRequest putObjectRequest = new PutObjectRequest(key.getBucket(), key.getKey(), new ByteArrayInputStream(buf, 0, count), objectMetadata);
                    client.putObject(putObjectRequest);
                }
            };

            NodeUtil.storeNode(out, node, userKey -> userKey.getBytes(), storeKey -> storeKey.getKey().getBytes());

            // NOTE: This writes the buffered data to S3.
            out.close();
        }
        catch (final IOException e) {
            throw new NodeStoreException(e);
        }
    }

    @Override
    public void removeNode(final S3ObjectId key) {
        client.deleteObject(key.getBucket(), key.getKey());
    }

    @Override
    public void removeData(final S3ObjectId key) {
        client.deleteObject(key.getBucket(), key.getKey());
    }

    @Override
    public S3ObjectId generateKey(final Node<String, S3ObjectId> node, final InputStream inputStream) {
        return new S3ObjectId(bucket, prefix + "generatedkeys/"+UUID.randomUUID().toString());
    }

    @Override
    public S3ObjectId convert(final String key) {
        return new S3ObjectId(bucket, prefix + "userkeys/"+key);
    }
}
