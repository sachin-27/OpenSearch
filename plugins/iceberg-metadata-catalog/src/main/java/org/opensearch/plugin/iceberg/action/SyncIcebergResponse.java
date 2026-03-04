/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.iceberg.action;

import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Response from syncing an index to Iceberg catalog.
 */
public class SyncIcebergResponse extends BroadcastResponse {
    
    private int filesAdded;
    private int filesRemoved;
    private int filesKept;
    private int filesArchived;
    
    public SyncIcebergResponse(StreamInput in) throws IOException {
        super(in);
        filesAdded = in.readVInt();
        filesRemoved = in.readVInt();
        filesKept = in.readVInt();
        filesArchived = in.readVInt();
    }
    
    public SyncIcebergResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures,
        int filesAdded,
        int filesRemoved,
        int filesKept
    ) {
        this(totalShards, successfulShards, failedShards, shardFailures, filesAdded, filesRemoved, filesKept, 0);
    }
    
    public SyncIcebergResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures,
        int filesAdded,
        int filesRemoved,
        int filesKept,
        int filesArchived
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.filesAdded = filesAdded;
        this.filesRemoved = filesRemoved;
        this.filesKept = filesKept;
        this.filesArchived = filesArchived;
    }
    
    public int getFilesAdded() {
        return filesAdded;
    }
    
    public int getFilesRemoved() {
        return filesRemoved;
    }
    
    public int getFilesKept() {
        return filesKept;
    }
    
    public int getFilesArchived() {
        return filesArchived;
    }
    
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(filesAdded);
        out.writeVInt(filesRemoved);
        out.writeVInt(filesKept);
        out.writeVInt(filesArchived);
    }
    
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        super.toXContent(builder, params);
        builder.field("files_added", filesAdded);
        builder.field("files_removed", filesRemoved);
        builder.field("files_kept", filesKept);
        builder.field("files_archived", filesArchived);
        builder.endObject();
        return builder;
    }
}
