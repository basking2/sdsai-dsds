package com.github.basking2.sdsai.dsds.s3;

import com.github.basking2.sdsai.dsds.node.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.utils.IoUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.UUID;

public class S3NodeStore implements NodeStore<String, S3ObjectId, String> {

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(S3NodeStore.class);

    /**
     * How S3 is accessed.
     */
    private final S3Client client;

    /**
     * The prefix all node keys will be placed under.
     */
    private final String nodePrefix;

    /**
     * The prefix all data keys will be placed under.
     */
    private final String dataPrefix;

    /**
     * The bucket keys will in.
     */
    private final String bucket;

    /**
     * Constructor.
     * @param client How to access S3.
     * @param bucket The bucket.
     * @param nodePrefix The key prefix that keys will be put under. This should probably end in a / as no / is inserted.
     * @param dataPrefix The key prefix that keys will be put under. This should probably end in a / as no / is inserted.
     */
    public S3NodeStore(final S3Client client, final String bucket, final String nodePrefix, final String dataPrefix) {
        this.client = client;
        this.bucket = bucket;
        this.nodePrefix = nodePrefix;
        this.dataPrefix = dataPrefix;
    }

    /**
     * Constructor.
     * @param client How to access S3.
     * @param bucket The bucket.
     * @param prefix The key prefix that keys will be put under. This should probably end in a / as no / is inserted.
     */
    public S3NodeStore(final S3Client client, final String bucket, final String prefix) {
        this.client = client;
        this.bucket = bucket;
        this.nodePrefix = prefix;
        this.dataPrefix = prefix;
    }

    /**
     * Use the default S3 client but otherwise the same constructor.
     * @param bucket The bucket.
     * @param prefix The key prefix that keys will be put under. This should probably end in a / as no / is inserted.
     */
    public S3NodeStore(final String bucket, final String prefix) {
        this(S3Client.builder().build(), bucket, prefix);
    }

    /**
     * The returned {@link S3Object} <b>must</b> be closed.
     *
     * @param key The key.
     * @return A freshly fetched {@link S3Object}.
     */
    @Override
    public String loadData(final S3ObjectId key) {
        try (final InputStream is = client.getObject(GetObjectRequest.builder().bucket(key.getBucket()).key(dataPrefix + key.getKey()).build())) {
            return IoUtils.toUtf8String(is);
        }
        catch(final NoSuchKeyException e) {
            return null;
        }
        catch(final S3Exception e) {
            throw new NodeStoreException("Loading key "+dataPrefix+key.getKey(), e);
        }
        catch (final Throwable t) {
            throw new NodeStoreException("Loading key "+dataPrefix+key.getKey(), t);
        }
    }

    @Override
    public Node<String, S3ObjectId> loadNode(final S3ObjectId key) {
        try (final ResponseInputStream<GetObjectResponse> o = client.getObject(GetObjectRequest.builder().bucket(key.getBucket()).key(nodePrefix + key.getKey()).build())) {
            return NodeUtil.readNode(
                    o,
                    userKey -> new String(userKey),
                    storekey -> new S3ObjectId(bucket, new String(storekey))
            );
        }
        catch(final NoSuchKeyException e) {
            throw new NodeStoreNodeNotFoundException(e);
        }
        catch(final S3Exception e) {
            throw new NodeStoreException(e);
        }
        catch (final Throwable t) {
            throw new NodeStoreException(t);
        }
    }

    @Override
    public void store(final S3ObjectId key, final String data) {
        client.putObject(
                PutObjectRequest.builder().bucket(key.getBucket()).key(dataPrefix + key.getKey()).build(),
                RequestBody.fromString(data)
        );
    }

    @Override
    public void store(final S3ObjectId key, final Node<String, S3ObjectId> node) {
        try {
            final ByteArrayOutputStream out = new ByteArrayOutputStream() {
                @Override
                public void close() throws IOException {
                    client.putObject(
                            PutObjectRequest
                                    .builder()
                                    .bucket(key.getBucket())
                                    .key(key.getKey())
                                    .build(),
                            RequestBody.fromByteBuffer(ByteBuffer.wrap(buf, 0, count))

                    );
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
        client.deleteObject(
                DeleteObjectRequest
                        .builder()
                        .bucket(key.getBucket())
                        .key(nodePrefix + key.getKey())
                        .build()
        );
    }

    @Override
    public void removeData(final S3ObjectId key) {
        client.deleteObject(
                DeleteObjectRequest
                        .builder()
                        .bucket(key.getBucket())
                        .key(dataPrefix + key.getKey())
                        .build()
        );
    }

    @Override
    public S3ObjectId generateKey(final Node<String, S3ObjectId> node, final String str) {
        return new S3ObjectId(bucket, UUID.randomUUID().toString());
    }

    @Override
    public S3ObjectId convert(final String key) {
        return new S3ObjectId(bucket, key);
    }
}
