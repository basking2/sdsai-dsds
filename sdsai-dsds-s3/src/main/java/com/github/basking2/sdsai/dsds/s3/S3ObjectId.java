package com.github.basking2.sdsai.dsds.s3;

public class S3ObjectId {
    private final String key;
    private final String bucket;

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public S3ObjectId(final String bucket, final String key) {
        this.key = key;
        this.bucket = bucket;
    }
}
