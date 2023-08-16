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
        private readonly IExportClient<OtlpCollector.ExportMetricsServiceRequest> exportClient;

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
        }

        internal OtlpResource.Resource ProcessResource => this.processResource ??= this.ParentProvider.GetResource().ToOtlpResource();

        /// <inheritdoc />
        public override ExportResult Export(in Batch<Metric> metrics)
        {
            using var scope = SuppressInstrumentationScope.Begin();

            var perMetricBatches = ArrayPool<List<OtlpMetrics.Metric>>.Shared.Rent((int)metrics.Count);
            var meterNames = ArrayPool<Metric>.Shared.Rent((int)metrics.Count);

            var i = 0;
            foreach (var metric in metrics)
            {
                perMetricBatches[i] = metric.ToBatchedOtlpMetric().ToList();
                meterNames[i] = metric;
                ++i;
            }

            var done = false;
            var num_msg = 0;
            while (!done)
            {
                var request = new OtlpCollector.ExportMetricsServiceRequest();

                for (i = 0; i < perMetricBatches.Length; ++i)
                {
                    done = true;

                    var messages = perMetricBatches[i];
                    if (messages.Count > 0)
                    {
                        var m = messages[messages.Count - 1];
                        messages.RemoveAt(messages.Count - 1);
                        request.AddMetrics(this.ProcessResource, m, meterNames[i]);
                        done = false;
                        num_msg++;
                    }
                }

                try
                {
                    if (!this.exportClient.SendExportRequest(request))
                    {
                        return ExportResult.Failure;
                    }
                    else
                    {
                        Thread.Sleep(1000);
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
                    ArrayPool<Metric>.Shared.Return(meterNames);
                    request.Return();
                }
            }

            return ExportResult.Success;
        }

        /// <inheritdoc />
        protected override bool OnShutdown(int timeoutMilliseconds)
        {
            return this.exportClient?.Shutdown(timeoutMilliseconds) ?? true;
        }
    }
}
