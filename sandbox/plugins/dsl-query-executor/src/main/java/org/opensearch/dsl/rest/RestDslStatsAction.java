/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.dsl.profile.DslPhaseStats;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Reads the node-local DSL phase timers collected by {@link DslPhaseStats}.
 *
 * <p>{@code GET /_dsl/stats} returns the current values; {@code POST /_dsl/stats/_reset} clears
 * them, so a benchmark run can be bracketed by a reset and a read. Timers are per-JVM and are not
 * aggregated across nodes — read them on the node under measurement.
 */
public class RestDslStatsAction extends BaseRestHandler {

    /** Creates the stats handler. */
    public RestDslStatsAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_dsl/stats"), new Route(POST, "/_dsl/stats/_reset"));
    }

    @Override
    public String getName() {
        return "dsl_phase_stats_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        boolean reset = request.method() == POST;
        Map<String, Object> snapshot = DslPhaseStats.snapshot();
        if (reset) {
            DslPhaseStats.reset();
        }
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("reset", reset);
                for (Map.Entry<String, Object> entry : snapshot.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        };
    }
}
