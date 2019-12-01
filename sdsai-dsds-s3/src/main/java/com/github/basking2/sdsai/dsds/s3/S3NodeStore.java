package com.github.basking2.sdsai.dsds.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import com.github.basking2.sdsai.dsds.node.*;

import java.io.*;
import java.util.UUID;

public class S3NodeStore implements NodeStore<String, S3ObjectId, String> {
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
    public String loadData(final S3ObjectId key) {
        try (final InputStream is = client.getObject(new GetObjectRequest(key.getBucket(), prefix + key.getKey())).getObjectContent()) {
            return IOUtils.toString(is);
        }
        catch(final AmazonS3Exception e) {
            if (e.getErrorCode().equals("NoSuchKey")) {
                throw new NodeStoreNodeNotFoundException(e);
            }
            else {
                throw new NodeStoreException("Loading key "+prefix+key.getKey(), e);
            }
        }
        catch (final Throwable t) {
            throw new NodeStoreException("Loading key "+prefix+key.getKey(), t);
        }
    }

    @Override
    public Node<String, S3ObjectId> loadNode(final S3ObjectId key) {
        try (final S3Object o = client.getObject(new GetObjectRequest(key.getBucket(), prefix + key.getKey()))) {
            return NodeUtil.readNode(
                    o.getObjectContent(),
                    userKey -> new String(userKey),
                    storekey -> new S3ObjectId(bucket, new String(storekey))
            );
        }
        catch(final AmazonS3Exception e) {
            if (e.getErrorCode().equals("NoSuchKey")) {
                throw new NodeStoreNodeNotFoundException(e);
            } else {
                throw new NodeStoreException(e);
            }
        }
        catch (final Throwable t) {
            throw new NodeStoreException(t);
        }
    }

    @Override
    public void store(final S3ObjectId key, final String data) {
        client.putObject(key.getKey(), prefix + key.getKey(), data);
    }

    @Override
    public void store(final S3ObjectId key, final Node<String, S3ObjectId> node) {
        try {
            final ByteArrayOutputStream out = new ByteArrayOutputStream() {
                @Override
                public void close() throws IOException {
                    final ObjectMetadata objectMetadata = new ObjectMetadata();
                    objectMetadata.setContentLength(count);
                    final PutObjectRequest putObjectRequest = new PutObjectRequest(key.getBucket(), prefix + key.getKey(), new ByteArrayInputStream(buf, 0, count), objectMetadata);
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
        client.deleteObject(key.getBucket(), prefix + key.getKey());
    }

    @Override
    public void removeData(final S3ObjectId key) {
        client.deleteObject(key.getBucket(), prefix + key.getKey());
    }

    @Override
    public S3ObjectId generateKey(final Node<String, S3ObjectId> node, final String str) {
        return new S3ObjectId(bucket, prefix + UUID.randomUUID().toString());
    }

    @Override
    public S3ObjectId convert(final String key) {
        return new S3ObjectId(bucket, prefix + key);
    }
}
