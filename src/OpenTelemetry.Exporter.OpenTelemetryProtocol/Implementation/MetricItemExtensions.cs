// <copyright file="MetricItemExtensions.cs" company="OpenTelemetry Authors">
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

using System.Collections.Concurrent;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using Google.Protobuf.Collections;
using OpenTelemetry.Metrics;
using OtlpCollector = OpenTelemetry.Proto.Collector.Metrics.V1;
using OtlpCommon = OpenTelemetry.Proto.Common.V1;
using OtlpMetrics = OpenTelemetry.Proto.Metrics.V1;
using OtlpResource = OpenTelemetry.Proto.Resource.V1;

namespace OpenTelemetry.Exporter.OpenTelemetryProtocol.Implementation
{
    internal static class MetricItemExtensions
    {
        private static readonly ConcurrentBag<OtlpMetrics.ScopeMetrics> MetricListPool = new();
        private static readonly Action<RepeatedField<OtlpMetrics.Metric>, int> RepeatedFieldOfMetricSetCountAction = CreateRepeatedFieldOfMetricSetCountAction();

        internal static void AddMetrics(
            this OtlpCollector.ExportMetricsServiceRequest request,
            OtlpResource.Resource processResource,
            in OtlpMetrics.Metric otlpMetric,
            in Metric metric)
        {
            var metricsByLibrary = new Dictionary<string, OtlpMetrics.ScopeMetrics>();

            if (request.ResourceMetrics.Count == 0)
            {
                var r = new OtlpMetrics.ResourceMetrics
                {
                    Resource = processResource,
                };

                request.ResourceMetrics.Add(r);
            }

            var resourceMetrics = request.ResourceMetrics.FirstOrDefault();

            // TODO: Replace null check with exception handling.
            if (otlpMetric == null)
            {
                OpenTelemetryProtocolExporterEventSource.Log.CouldNotTranslateMetric(
                    nameof(MetricItemExtensions),
                    nameof(AddMetrics));
                return;
            }

            var meterName = metric.MeterName;
            if (!metricsByLibrary.TryGetValue(meterName, out var scopeMetrics))
            {
                scopeMetrics = GetMetricListFromPool(meterName, metric.MeterVersion);

                metricsByLibrary.Add(meterName, scopeMetrics);
                resourceMetrics.ScopeMetrics.Add(scopeMetrics);
            }

            scopeMetrics.Metrics.Add(otlpMetric);
        }

        internal static void AddMetrics(
            this OtlpCollector.ExportMetricsServiceRequest request,
            OtlpResource.Resource processResource,
            in Batch<Metric> metrics)
        {
            var metricsByLibrary = new Dictionary<string, OtlpMetrics.ScopeMetrics>();
            var resourceMetrics = new OtlpMetrics.ResourceMetrics
            {
                Resource = processResource,
            };
            request.ResourceMetrics.Add(resourceMetrics);

            foreach (var metric in metrics)
            {
                var otlpMetric = metric.ToOtlpMetric();

                // TODO: Replace null check with exception handling.
                if (otlpMetric == null)
                {
                    OpenTelemetryProtocolExporterEventSource.Log.CouldNotTranslateMetric(
                        nameof(MetricItemExtensions),
                        nameof(AddMetrics));
                    continue;
                }

                var meterName = metric.MeterName;
                if (!metricsByLibrary.TryGetValue(meterName, out var scopeMetrics))
                {
                    scopeMetrics = GetMetricListFromPool(meterName, metric.MeterVersion);

                    metricsByLibrary.Add(meterName, scopeMetrics);
                    resourceMetrics.ScopeMetrics.Add(scopeMetrics);
                }

                scopeMetrics.Metrics.Add(otlpMetric);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Return(this OtlpCollector.ExportMetricsServiceRequest request)
        {
            var resourceMetrics = request.ResourceMetrics.FirstOrDefault();
            if (resourceMetrics == null)
            {
                return;
            }

            foreach (var scope in resourceMetrics.ScopeMetrics)
            {
                RepeatedFieldOfMetricSetCountAction(scope.Metrics, 0);
                MetricListPool.Add(scope);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static OtlpMetrics.ScopeMetrics GetMetricListFromPool(string name, string version)
        {
            if (!MetricListPool.TryTake(out var metrics))
            {
                metrics = new OtlpMetrics.ScopeMetrics
                {
                    Scope = new OtlpCommon.InstrumentationScope
                    {
                        Name = name,
                        Version = version ?? string.Empty,
                    },
                };
            }
            else
            {
                metrics.Scope.Name = name;
                metrics.Scope.Version = version ?? string.Empty;
            }

            return metrics;
        }

        internal static OtlpMetrics.Metric GetMetric(Metric metric)
        {
            var otlpMetric = new OtlpMetrics.Metric
            {
                Name = metric.Name,
            };

            if (metric.Description != null)
            {
                otlpMetric.Description = metric.Description;
            }

            if (metric.Unit != null)
            {
                otlpMetric.Unit = metric.Unit;
            }

            return otlpMetric;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IEnumerable<OtlpMetrics.Metric> ToBatchedOtlpMetric(this Metric metric, int batchSize = 15000)
        {
            List<OtlpMetrics.Metric> metrics = new();
            var currentBatch = 0;

            OtlpMetrics.AggregationTemporality temporality;
            if (metric.Temporality == AggregationTemporality.Delta)
            {
                temporality = OtlpMetrics.AggregationTemporality.Delta;
            }
            else
            {
                temporality = OtlpMetrics.AggregationTemporality.Cumulative;
            }

            var otlpMetric = GetMetric(metric);

            switch (metric.MetricType)
            {
                case MetricType.LongSum:
                case MetricType.LongSumNonMonotonic:
                    {
                        var sum = new OtlpMetrics.Sum
                        {
                            IsMonotonic = metric.MetricType == MetricType.LongSum,
                            AggregationTemporality = temporality,
                        };

                        foreach (ref readonly var metricPoint in metric.GetMetricPoints())
                        {
                            currentBatch++;
                            var dataPoint = GetNumberDataPoint(metricPoint);
                            dataPoint.AsInt = metricPoint.GetSumLong();
                            sum.DataPoints.Add(dataPoint);

                            if (currentBatch >= batchSize)
                            {
                                OffLoadMetricSum(metric, metrics, ref otlpMetric, ref sum);
                                currentBatch = 0;
                            }
                        }

                        if (currentBatch > 0)
                        {
                            otlpMetric.Sum = sum;
                            metrics.Add(otlpMetric);
                        }

                        break;
                    }

                case MetricType.DoubleSum:
                case MetricType.DoubleSumNonMonotonic:
                    {
                        var sum = new OtlpMetrics.Sum
                        {
                            IsMonotonic = metric.MetricType == MetricType.DoubleSum,
                            AggregationTemporality = temporality,
                        };

                        foreach (ref readonly var metricPoint in metric.GetMetricPoints())
                        {
                            currentBatch++;
                            var dataPoint = GetNumberDataPoint(metricPoint);
                            dataPoint.AsDouble = metricPoint.GetSumDouble();
                            sum.DataPoints.Add(dataPoint);

                            if (currentBatch >= batchSize)
                            {
                                OffLoadMetricSum(metric, metrics, ref otlpMetric, ref sum);
                                currentBatch = 0;
                            }
                        }

                        if (currentBatch > 0)
                        {
                            otlpMetric.Sum = sum;
                            metrics.Add(otlpMetric);
                        }

                        break;
                    }

                case MetricType.LongGauge:
                    {
                        var gauge = new OtlpMetrics.Gauge();
                        foreach (ref readonly var metricPoint in metric.GetMetricPoints())
                        {
                            currentBatch++;
                            var dataPoint = GetNumberDataPoint(metricPoint);
                            dataPoint.AsInt = metricPoint.GetGaugeLastValueLong();
                            gauge.DataPoints.Add(dataPoint);

                            if (currentBatch >= batchSize)
                            {
                                OffLoadMetricGauge(metric, metrics, ref otlpMetric, ref gauge);
                                currentBatch = 0;
                            }
                        }

                        if (currentBatch > 0)
                        {
                            otlpMetric.Gauge = gauge;
                            metrics.Add(otlpMetric);
                        }

                        break;
                    }

                case MetricType.DoubleGauge:
                    {
                        var gauge = new OtlpMetrics.Gauge();
                        foreach (ref readonly var metricPoint in metric.GetMetricPoints())
                        {
                            currentBatch++;
                            var dataPoint = GetNumberDataPoint(metricPoint);
                            dataPoint.AsDouble = metricPoint.GetGaugeLastValueDouble();
                            gauge.DataPoints.Add(dataPoint);

                            if (currentBatch >= batchSize)
                            {
                                OffLoadMetricGauge(metric, metrics, ref otlpMetric, ref gauge);
                                currentBatch = 0;
                            }
                        }

                        if (currentBatch > 0)
                        {
                            otlpMetric.Gauge = gauge;
                            metrics.Add(otlpMetric);
                        }

                        break;
                    }

                case MetricType.Histogram:
                    {
                        var histogram = new OtlpMetrics.Histogram
                        {
                            AggregationTemporality = temporality,
                        };

                        foreach (ref readonly var metricPoint in metric.GetMetricPoints())
                        {
                            currentBatch++;
                            var dataPoint = GetHistogramDataPoint(metricPoint);
                            dataPoint.Count = (ulong)metricPoint.GetHistogramCount();
                            dataPoint.Sum = metricPoint.GetHistogramSum();

                            if (metricPoint.TryGetHistogramMinMaxValues(out double min, out double max))
                            {
                                dataPoint.Min = min;
                                dataPoint.Max = max;
                            }

                            foreach (var histogramMeasurement in metricPoint.GetHistogramBuckets())
                            {
                                dataPoint.BucketCounts.Add((ulong)histogramMeasurement.BucketCount);
                                if (histogramMeasurement.ExplicitBound != double.PositiveInfinity)
                                {
                                    dataPoint.ExplicitBounds.Add(histogramMeasurement.ExplicitBound);
                                }
                            }

                            histogram.DataPoints.Add(dataPoint);

                            if (currentBatch >= batchSize)
                            {
                                OffLoadMetricHistogram(metric, metrics, temporality, ref otlpMetric, ref histogram);
                                currentBatch = 0;
                            }
                        }

                        if (currentBatch > 0)
                        {
                            otlpMetric.Histogram = histogram;
                            metrics.Add(otlpMetric);
                        }

                        break;
                    }
            }

            return metrics;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static OtlpMetrics.Metric ToOtlpMetric(this Metric metric)
        {
            var otlpMetric = new OtlpMetrics.Metric
            {
                Name = metric.Name,
            };

            if (metric.Description != null)
            {
                otlpMetric.Description = metric.Description;
            }

            if (metric.Unit != null)
            {
                otlpMetric.Unit = metric.Unit;
            }

            OtlpMetrics.AggregationTemporality temporality;
            if (metric.Temporality == AggregationTemporality.Delta)
            {
                temporality = OtlpMetrics.AggregationTemporality.Delta;
            }
            else
            {
                temporality = OtlpMetrics.AggregationTemporality.Cumulative;
            }

            switch (metric.MetricType)
            {
                case MetricType.LongSum:
                case MetricType.LongSumNonMonotonic:
                    {
                        var sum = new OtlpMetrics.Sum
                        {
                            IsMonotonic = metric.MetricType == MetricType.LongSum,
                            AggregationTemporality = temporality,
                        };

                        foreach (ref readonly var metricPoint in metric.GetMetricPoints())
                        {
                            var dataPoint = GetNumberDataPoint(metricPoint);
                            dataPoint.AsInt = metricPoint.GetSumLong();
                            sum.DataPoints.Add(dataPoint);
                        }

                        otlpMetric.Sum = sum;
                        break;
                    }

                case MetricType.DoubleSum:
                case MetricType.DoubleSumNonMonotonic:
                    {
                        var sum = new OtlpMetrics.Sum
                        {
                            IsMonotonic = metric.MetricType == MetricType.DoubleSum,
                            AggregationTemporality = temporality,
                        };

                        foreach (ref readonly var metricPoint in metric.GetMetricPoints())
                        {
                            var dataPoint = GetNumberDataPoint(metricPoint);
                            dataPoint.AsDouble = metricPoint.GetSumDouble();
                            sum.DataPoints.Add(dataPoint);
                        }

                        otlpMetric.Sum = sum;
                        break;
                    }

                case MetricType.LongGauge:
                    {
                        var gauge = new OtlpMetrics.Gauge();
                        foreach (ref readonly var metricPoint in metric.GetMetricPoints())
                        {
                            var dataPoint = GetNumberDataPoint(metricPoint);
                            dataPoint.AsInt = metricPoint.GetGaugeLastValueLong();
                            gauge.DataPoints.Add(dataPoint);
                        }

                        otlpMetric.Gauge = gauge;
                        break;
                    }

                case MetricType.DoubleGauge:
                    {
                        var gauge = new OtlpMetrics.Gauge();
                        foreach (ref readonly var metricPoint in metric.GetMetricPoints())
                        {
                            var dataPoint = GetNumberDataPoint(metricPoint);
                            dataPoint.AsDouble = metricPoint.GetGaugeLastValueDouble();
                            gauge.DataPoints.Add(dataPoint);
                        }

                        otlpMetric.Gauge = gauge;
                        break;
                    }

                case MetricType.Histogram:
                    {
                        var histogram = new OtlpMetrics.Histogram
                        {
                            AggregationTemporality = temporality,
                        };

                        foreach (ref readonly var metricPoint in metric.GetMetricPoints())
                        {
                            var dataPoint = GetHistogramDataPoint(metricPoint);
                            dataPoint.Count = (ulong)metricPoint.GetHistogramCount();
                            dataPoint.Sum = metricPoint.GetHistogramSum();

                            if (metricPoint.TryGetHistogramMinMaxValues(out double min, out double max))
                            {
                                dataPoint.Min = min;
                                dataPoint.Max = max;
                            }

                            foreach (var histogramMeasurement in metricPoint.GetHistogramBuckets())
                            {
                                dataPoint.BucketCounts.Add((ulong)histogramMeasurement.BucketCount);
                                if (histogramMeasurement.ExplicitBound != double.PositiveInfinity)
                                {
                                    dataPoint.ExplicitBounds.Add(histogramMeasurement.ExplicitBound);
                                }
                            }

                            histogram.DataPoints.Add(dataPoint);
                        }

                        otlpMetric.Histogram = histogram;
                        break;
                    }
            }

            return otlpMetric;
        }

        private static void OffLoadMetricHistogram(Metric metric, List<OtlpMetrics.Metric> metrics, OtlpMetrics.AggregationTemporality temporality, ref OtlpMetrics.Metric otlpMetric, ref OtlpMetrics.Histogram histogram)
        {
            otlpMetric.Histogram = histogram;
            metrics.Add(otlpMetric);
            histogram = new OtlpMetrics.Histogram
            {
                AggregationTemporality = temporality,
            };
            otlpMetric = GetMetric(metric);
        }

        private static void OffLoadMetricSum(Metric metric, List<OtlpMetrics.Metric> metrics, ref OtlpMetrics.Metric otlpMetric, ref OtlpMetrics.Sum sum)
        {
            otlpMetric.Sum = sum;
            metrics.Add(otlpMetric);
            sum = new OtlpMetrics.Sum();
            otlpMetric = GetMetric(metric);
        }

        private static void OffLoadMetricGauge(Metric metric, List<OtlpMetrics.Metric> metrics, ref OtlpMetrics.Metric otlpMetric, ref OtlpMetrics.Gauge gauge)
        {
            otlpMetric.Gauge = gauge;
            metrics.Add(otlpMetric);
            gauge = new OtlpMetrics.Gauge();
            otlpMetric = GetMetric(metric);
        }

        private static OtlpMetrics.HistogramDataPoint GetHistogramDataPoint(MetricPoint metricPoint)
        {
            var dataPoint = new OtlpMetrics.HistogramDataPoint
            {
                StartTimeUnixNano = (ulong)metricPoint.StartTime.ToUnixTimeNanoseconds(),
                TimeUnixNano = (ulong)metricPoint.EndTime.ToUnixTimeNanoseconds(),
            };

            AddAttributes(metricPoint.Tags, dataPoint.Attributes);
            return dataPoint;
        }

        private static OtlpMetrics.NumberDataPoint GetNumberDataPoint(MetricPoint metricPoint)
        {
            var dataPoint = new OtlpMetrics.NumberDataPoint
            {
                StartTimeUnixNano = (ulong)metricPoint.StartTime.ToUnixTimeNanoseconds(),
                TimeUnixNano = (ulong)metricPoint.EndTime.ToUnixTimeNanoseconds(),
            };

            AddAttributes(metricPoint.Tags, dataPoint.Attributes);
            return dataPoint;
        }

        private static void AddAttributes(ReadOnlyTagCollection tags, RepeatedField<OtlpCommon.KeyValue> attributes)
        {
            foreach (var tag in tags)
            {
                if (OtlpKeyValueTransformer.Instance.TryTransformTag(tag, out var result))
                {
                    attributes.Add(result);
                }
            }
        }

        private static Action<RepeatedField<OtlpMetrics.Metric>, int> CreateRepeatedFieldOfMetricSetCountAction()
        {
            FieldInfo repeatedFieldOfMetricCountField = typeof(RepeatedField<OtlpMetrics.Metric>).GetField("count", BindingFlags.NonPublic | BindingFlags.Instance);

            DynamicMethod dynamicMethod = new DynamicMethod(
                "CreateSetCountAction",
                null,
                new[] { typeof(RepeatedField<OtlpMetrics.Metric>), typeof(int) },
                typeof(MetricItemExtensions).Module,
                skipVisibility: true);

            var generator = dynamicMethod.GetILGenerator();

            generator.Emit(OpCodes.Ldarg_0);
            generator.Emit(OpCodes.Ldarg_1);
            generator.Emit(OpCodes.Stfld, repeatedFieldOfMetricCountField);
            generator.Emit(OpCodes.Ret);

            return (Action<RepeatedField<OtlpMetrics.Metric>, int>)dynamicMethod.CreateDelegate(typeof(Action<RepeatedField<OtlpMetrics.Metric>, int>));
        }
    }
}
