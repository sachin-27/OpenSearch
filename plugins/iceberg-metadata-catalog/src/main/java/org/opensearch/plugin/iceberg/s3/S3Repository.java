/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.iceberg.s3;
// Copied from plugins/repository-s3/ for plugin isolation. Tech debt: extract to shared library.

/**
 * Inlined Setting constants from S3Repository (plugins/repository-s3).
 * These are needed by the copied S3 client code (S3BlobStore, S3BlobContainer, S3ClientSettings, S3AsyncService, etc.)
 * which originally referenced S3Repository directly.
 *
 * This class is NOT an actual repository — it just holds the Setting definitions
 * so the copied S3 code compiles without depending on the repository-s3 plugin.
 */

import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.monitor.jvm.JvmInfo;

import java.util.function.Function;

public final class S3Repository {

    private S3Repository() {}

    /** The access key to authenticate with s3. This setting is insecure because cluster settings are stored in cluster state */
    public static final Setting<SecureString> ACCESS_KEY_SETTING = SecureSetting.insecureString("access_key");

    /** The secret key to authenticate with s3. This setting is insecure because cluster settings are stored in cluster state */
    public static final Setting<SecureString> SECRET_KEY_SETTING = SecureSetting.insecureString("secret_key");

    /**
     * Default is to use 100MB (S3 defaults) for heaps above 2GB and 5% of
     * the available memory for smaller heaps.
     */
    private static final ByteSizeValue DEFAULT_BUFFER_SIZE = new ByteSizeValue(
        Math.max(
            ByteSizeUnit.MB.toBytes(5),
            Math.min(ByteSizeUnit.MB.toBytes(100), JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 20)
        ),
        ByteSizeUnit.BYTES
    );

    private static final ByteSizeValue DEFAULT_MULTIPART_UPLOAD_MINIMUM_PART_SIZE = new ByteSizeValue(
        ByteSizeUnit.MB.toBytes(16),
        ByteSizeUnit.BYTES
    );

    public static final Setting<String> BUCKET_SETTING = Setting.simpleString("bucket");

    public static final String BUCKET_DEFAULT_ENCRYPTION_TYPE = "bucket_default";

    public static final String NETTY_ASYNC_HTTP_CLIENT_TYPE = "netty";
    public static final String CRT_ASYNC_HTTP_CLIENT_TYPE = "crt";

    public static final Setting<String> SERVER_SIDE_ENCRYPTION_TYPE_SETTING = Setting.simpleString(
        "server_side_encryption_type",
        BUCKET_DEFAULT_ENCRYPTION_TYPE
    );

    public static final Setting<String> SERVER_SIDE_ENCRYPTION_KMS_KEY_SETTING = Setting.simpleString("server_side_encryption_kms_key_id");

    public static final Setting<Boolean> SERVER_SIDE_ENCRYPTION_BUCKET_KEY_SETTING = Setting.boolSetting(
        "server_side_encryption_bucket_key_enabled",
        true
    );

    public static final Setting<String> SERVER_SIDE_ENCRYPTION_ENCRYPTION_CONTEXT_SETTING = Setting.simpleString(
        "server_side_encryption_encryption_context"
    );

    public static final Setting<String> EXPECTED_BUCKET_OWNER_SETTING = Setting.simpleString("expected_bucket_owner");

    public static final Setting<String> S3_ASYNC_HTTP_CLIENT_TYPE = Setting.simpleString(
        "s3_async_client_type",
        CRT_ASYNC_HTTP_CLIENT_TYPE,
        Setting.Property.NodeScope
    );

    /** Maximum size of files that can be uploaded using a single upload request. */
    public static final ByteSizeValue MAX_FILE_SIZE = new ByteSizeValue(5, ByteSizeUnit.GB);

    /** Minimum size of parts that can be uploaded using the Multipart Upload API. */
    public static final ByteSizeValue MIN_PART_SIZE_USING_MULTIPART = new ByteSizeValue(5, ByteSizeUnit.MB);

    /** Maximum size of parts that can be uploaded using the Multipart Upload API. */
    public static final ByteSizeValue MAX_PART_SIZE_USING_MULTIPART = MAX_FILE_SIZE;

    /** Maximum size of files that can be uploaded using the Multipart Upload API. */
    public static final ByteSizeValue MAX_FILE_SIZE_USING_MULTIPART = new ByteSizeValue(5, ByteSizeUnit.TB);

    public static final Setting<Boolean> REDIRECT_LARGE_S3_UPLOAD = Setting.boolSetting(
        "redirect_large_s3_upload",
        true,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> PERMIT_BACKED_TRANSFER_ENABLED = Setting.boolSetting(
        "permit_backed_transfer_enabled",
        true,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> UPLOAD_RETRY_ENABLED = Setting.boolSetting(
        "s3_upload_retry_enabled",
        true,
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> BUFFER_SIZE_SETTING = Setting.byteSizeSetting(
        "buffer_size",
        DEFAULT_BUFFER_SIZE,
        MIN_PART_SIZE_USING_MULTIPART,
        MAX_PART_SIZE_USING_MULTIPART
    );

    public static final Setting<ByteSizeValue> PARALLEL_MULTIPART_UPLOAD_MINIMUM_PART_SIZE_SETTING = Setting.byteSizeSetting(
        "parallel_multipart_upload.minimum_part_size",
        DEFAULT_MULTIPART_UPLOAD_MINIMUM_PART_SIZE,
        MIN_PART_SIZE_USING_MULTIPART,
        MAX_PART_SIZE_USING_MULTIPART,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> PARALLEL_MULTIPART_UPLOAD_ENABLED_SETTING = Setting.boolSetting(
        "parallel_multipart_upload.enabled",
        true,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> BULK_DELETE_SIZE = Setting.intSetting("bulk_delete_size", 1000, 1, 1000);

    public static final Setting<String> STORAGE_CLASS_SETTING = Setting.simpleString("storage_class");

    public static final Setting<String> CANNED_ACL_SETTING = Setting.simpleString("canned_acl");

    public static final Setting<String> CLIENT_NAME = new Setting<>("client", "default", Function.identity());

    public static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path");
}
