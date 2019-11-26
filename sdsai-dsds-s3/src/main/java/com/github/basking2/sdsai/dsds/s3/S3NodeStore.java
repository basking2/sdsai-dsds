package com.github.basking2.sdsai.dsds.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class S3NodeStore {
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
}
