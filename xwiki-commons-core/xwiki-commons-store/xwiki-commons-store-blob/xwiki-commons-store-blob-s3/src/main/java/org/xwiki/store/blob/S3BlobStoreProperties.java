/*
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.xwiki.store.blob;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import org.xwiki.properties.annotation.PropertyId;
import org.xwiki.properties.annotation.PropertyMandatory;
import org.xwiki.stability.Unstable;

/**
 * Properties bean for S3 blob store.
 * This class defines the properties required for configuring an S3-based blob store.
 *
 * @version $Id$
 * @since 17.10.0RC1
 */
@Unstable
public class S3BlobStoreProperties implements BlobStoreProperties
{
    /**
     * The property ID for the S3 bucket.
     */
    public static final String BUCKET = "s3.bucket";

    /**
     * The property ID for the S3 key prefix. By default, the key prefix consists of the configured key prefix and
     * the name of the blob store.
     */
    public static final String KEY_PREFIX = "s3.keyPrefix";

    /**
     * The property ID for the S3 multipart upload part size.
     */
    public static final String MULTIPART_UPLOAD_PART_SIZE = "s3.uploadPartSizeBytes";

    /**
     * The property ID for the S3 multipart copy part size.
     */
    public static final String MULTIPART_COPY_PART_SIZE = "s3.copyPartSizeBytes";

    /**
     * The property ID indicating whether the S3 service supports the condition of uploads: {@code If-None-Match} on
     * {@code PutObject} and {@code CompleteMultipartUpload}.
     *
     * @since 18.10.0RC1
     */
    @Unstable
    public static final String CONDITIONAL_WRITES = "s3.conditionalWrites";

    /**
     * The property ID indicating whether the S3 service supports the conditions of copies: {@code If-None-Match} on
     * {@code CopyObject} and {@code x-amz-copy-source-if-match} on {@code UploadPartCopy}.
     *
     * @since 18.10.0RC1
     */
    @Unstable
    public static final String CONDITIONAL_COPIES = "s3.conditionalCopies";

    /**
     * The minimum part size for S3 multipart uploads (5 MiB).
     */
    private static final long MIN_PART_SIZE = 5L * 1024 * 1024;

    /**
     * The maximum part size for S3 multipart uploads (5 GiB).
     */
    private static final long MAX_PART_SIZE = 5L * 1024 * 1024 * 1024;

    /**
     * The S3 bucket name.
     */
    @NotBlank
    private String bucketName;

    /**
     * The S3 key prefix.
     */
    private String keyPrefix = "";

    /**
     * The part size for S3 multipart uploads.
     */
    @Min(MIN_PART_SIZE)
    @Max(MAX_PART_SIZE)
    private long multipartUploadPartSize;

    /**
     * The part size for S3 multipart copy operations.
     */
    @Min(MIN_PART_SIZE)
    @Max(MAX_PART_SIZE)
    private long multipartCopyPartSize;

    /**
     * Whether to send the condition of uploads.
     */
    private boolean conditionalWrites = true;

    /**
     * Whether to send the conditions of copies.
     */
    private boolean conditionalCopies = true;

    /**
     * Gets the S3 bucket name.
     *
     * @return the S3 bucket name
     */
    public String getBucket()
    {
        return this.bucketName;
    }

    /**
     * Sets the S3 bucket name.
     *
     * @param bucket the bucket name to set
     */
    @PropertyId(BUCKET)
    @PropertyMandatory
    public void setBucket(String bucket)
    {
        this.bucketName = bucket;
    }

    /**
     * Gets the S3 key prefix.
     *
     * @return the S3 key prefix
     */
    public String getKeyPrefix()
    {
        return this.keyPrefix;
    }

    /**
     * Sets the S3 key prefix.
     *
     * @param keyPrefix the key prefix to set
     */
    @PropertyId(KEY_PREFIX)
    public void setKeyPrefix(String keyPrefix)
    {
        this.keyPrefix = keyPrefix;
    }

    /**
     * Gets the part size for S3 multipart uploads.
     *
     * @return the multipart upload part size
     */
    public long getMultipartUploadPartSize()
    {
        return this.multipartUploadPartSize;
    }

    /**
     * Sets the part size for S3 multipart uploads.
     *
     * @param multipartUploadPartSize the part size to set
     */
    @PropertyId(MULTIPART_UPLOAD_PART_SIZE)
    public void setMultipartUploadPartSize(long multipartUploadPartSize)
    {
        this.multipartUploadPartSize = multipartUploadPartSize;
    }

    /**
     * Gets the part size for S3 multipart copy operations.
     *
     * @return the multipart copy part size
     */
    public long getMultipartCopyPartSize()
    {
        return this.multipartCopyPartSize;
    }

    /**
     * Sets the part size for S3 multipart copy operations.
     *
     * @param multipartCopyPartSize the copy part size to set
     */
    @PropertyId(MULTIPART_COPY_PART_SIZE)
    public void setMultipartCopyPartSize(long multipartCopyPartSize)
    {
        this.multipartCopyPartSize = multipartCopyPartSize;
    }

    /**
     * Indicates whether the S3 service supports the condition sent with uploads: {@code If-None-Match: *} on
     * {@code PutObject} and {@code CompleteMultipartUpload} (also used to complete multipart copies), which creates a
     * blob only if it does not exist yet ({@link BlobWriteMode#CREATE_NEW}). When disabled, the store relies on its
     * existence check alone, which is not atomic.
     *
     * @return {@code true} if the conditions are sent (the default), {@code false} otherwise
     * @since 18.10.0RC1
     */
    @Unstable
    public boolean isConditionalWrites()
    {
        return this.conditionalWrites;
    }

    /**
     * Sets whether the S3 service supports the condition sent with uploads.
     *
     * @param conditionalWrites {@code true} to send the conditions, {@code false} for services that do not support
     *     them
     * @see #isConditionalWrites()
     * @since 18.10.0RC1
     */
    @Unstable
    @PropertyId(CONDITIONAL_WRITES)
    public void setConditionalWrites(boolean conditionalWrites)
    {
        this.conditionalWrites = conditionalWrites;
    }

    /**
     * Indicates whether the S3 service supports the conditions sent with copies: {@code If-None-Match: *} on
     * {@code CopyObject} (blobs up to the multipart copy part size), which creates the target only if it does not
     * exist yet ({@link BlobWriteMode#CREATE_NEW}), and {@code x-amz-copy-source-if-match} on {@code UploadPartCopy}
     * (larger blobs), which makes sure that all parts are copied from the same version of the source. Some services
     * support the condition of uploads but reject or misjudge the conditions of copies. When disabled, copies up to the
     * multipart copy part size rely on the existence check of the target alone, which is not atomic (larger copies
     * complete with the condition of uploads, see {@link #isConditionalWrites()}), and the source of a multipart copy
     * is checked again before completing it.
     *
     * @return {@code true} if the conditions are sent (the default), {@code false} otherwise
     * @since 18.10.0RC1
     */
    @Unstable
    public boolean isConditionalCopies()
    {
        return this.conditionalCopies;
    }

    /**
     * Sets whether the S3 service supports the conditions sent with copies.
     *
     * @param conditionalCopies {@code true} to send the conditions, {@code false} for services that do not support
     *     them
     * @see #isConditionalCopies()
     * @since 18.10.0RC1
     */
    @Unstable
    @PropertyId(CONDITIONAL_COPIES)
    public void setConditionalCopies(boolean conditionalCopies)
    {
        this.conditionalCopies = conditionalCopies;
    }
}
