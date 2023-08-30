// <copyright file="OtlpMetricExporter.cs" company="OpenTelemetry Authors">
// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>

using System.Buffers;
using OpenTelemetry.Exporter.OpenTelemetryProtocol.Implementation;
using OpenTelemetry.Exporter.OpenTelemetryProtocol.Implementation.ExportClient;
using OpenTelemetry.Metrics;
using OtlpCollector = OpenTelemetry.Proto.Collector.Metrics.V1;
using OtlpMetrics = OpenTelemetry.Proto.Metrics.V1;
using OtlpResource = OpenTelemetry.Proto.Resource.V1;

namespace OpenTelemetry.Exporter
{
    /// <summary>
    /// Exporter consuming <see cref="Metric"/> and exporting the data using
    /// the OpenTelemetry protocol (OTLP).
    /// </summary>
    public class OtlpMetricExporter : BaseExporter<Metric>
    {
        private static readonly Random Random = new();

        private readonly IExportClient<OtlpCollector.ExportMetricsServiceRequest> exportClient;

        private readonly int dataPointBatchSize;

        private readonly bool dataPointbatchingEnabled;

        private readonly int dataPointBatchExportInterval;

        private readonly int maxJitter;

        private readonly int minJitter;

        private OtlpResource.Resource processResource;

        /// <summary>
        /// Initializes a new instance of the <see cref="OtlpMetricExporter"/> class.
        /// </summary>
        /// <param name="options">Configuration options for the exporter.</param>
        public OtlpMetricExporter(OtlpExporterOptions options)
            : this(options, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OtlpMetricExporter"/> class.
        /// </summary>
        /// <param name="options">Configuration options for the export.</param>
        /// <param name="exportClient">Client used for sending export request.</param>
        internal OtlpMetricExporter(OtlpExporterOptions options, IExportClient<OtlpCollector.ExportMetricsServiceRequest> exportClient = null)
        {
            if (exportClient != null)
            {
                this.exportClient = exportClient;
            }
            else
            {
                this.exportClient = options.GetMetricsExportClient();
            }

            this.dataPointBatchSize = options.DataPointBatchSize;
            this.dataPointbatchingEnabled = options.DataPointBatchingEnabled;
            this.dataPointBatchExportInterval = options.DataPointBatchExportInterval;
            this.maxJitter = options.MaxJitter;
            this.minJitter = options.MinJitter;
        }

        internal OtlpResource.Resource ProcessResource => this.processResource ??= this.ParentProvider.GetResource().ToOtlpResource();

        /// <inheritdoc />
        public override ExportResult Export(in Batch<Metric> metrics)
        {
            // Prevents the exporter's gRPC and HTTP operations from being instrumented.
            using var scope = SuppressInstrumentationScope.Begin();

            // [Performance] Need to allow for a message with partial data points but several metrics so we can still batch metrics in a single message
            //               and minimize the amount of requests to transmit all the data.
            var perMetricBatches = ArrayPool<List<OtlpMetrics.Metric>>.Shared.Rent((int)metrics.Count);
            var allMetrics = ArrayPool<Metric>.Shared.Rent((int)metrics.Count);

            var i = 0;
            foreach (var metric in metrics)
            {
                perMetricBatches[i] = metric.ToOtlpMetric(this.dataPointBatchSize, this.dataPointbatchingEnabled).ToList();
                allMetrics[i] = metric;
                ++i;
            }

            // [abeaulieu] Now loop through all the metrics until we've sent all the messages to send
            OtlpCollector.ExportMetricsServiceRequest request;
            bool mustWait = false;
            do
            {
                var currentBatchSize = 0;
                request = null;
                for (i = 0; i < metrics.Count; ++i)
                {
                    var messages = perMetricBatches[i];
                    if (messages.Count > 0)
                    {
                        request ??= new OtlpCollector.ExportMetricsServiceRequest();
                        var m = messages[messages.Count - 1]; // Take the last one so there's no need to list copy to shift the list left.
                        var dataPointCount = m.GetDataPointCount();

                        if (currentBatchSize + dataPointCount <= this.dataPointBatchSize) // if the current message fits in the request
                        {
                            currentBatchSize += dataPointCount;
                            messages.RemoveAt(messages.Count - 1); // [Perf]: Use a queue or just index manipulation to avoid memory shenanigans.
                            mustWait |= messages.Count > 0;
                            request.AddMetrics(this.ProcessResource, m, allMetrics[i]);
                        }
                        else
                        {
                            mustWait = true;
                            continue;
                        }
                    }
                }

                try
                {
                    if (request != null && !this.exportClient.SendExportRequest(request))
                    {
                        return ExportResult.Failure;
                    }

                    if (mustWait && this.dataPointBatchExportInterval > 0)
                    {
                        var delay = this.dataPointBatchExportInterval + Random.Next(this.minJitter, this.maxJitter);
                        Thread.Sleep(delay);
                    }
                }
                catch (Exception ex)
                {
                    OpenTelemetryProtocolExporterEventSource.Log.ExportMethodException(ex);
                    return ExportResult.Failure;
                }
                finally
                {
                    ArrayPool<List<OtlpMetrics.Metric>>.Shared.Return(perMetricBatches);
                    ArrayPool<Metric>.Shared.Return(allMetrics);
                    request?.Return();
                    mustWait = false;
                }
            }
            while (request != null);

            return ExportResult.Success;
        }

        /// <inheritdoc />
        protected override bool OnShutdown(int timeoutMilliseconds)
        {
            return this.exportClient?.Shutdown(timeoutMilliseconds) ?? true;
        }
    }
}
