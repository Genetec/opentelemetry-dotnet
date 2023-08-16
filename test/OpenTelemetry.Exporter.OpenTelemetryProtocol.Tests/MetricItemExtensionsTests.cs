// <copyright file="MetricItemExtensionsTests.cs" company="OpenTelemetry Authors">
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

using System.Diagnostics.Metrics;
using OpenTelemetry.Exporter.OpenTelemetryProtocol.Implementation;
using OpenTelemetry.Metrics;
using OpenTelemetry.Tests;
using Xunit;

namespace OpenTelemetry.Exporter.OpenTelemetryProtocol.Tests
{
    public class MetricItemExtensionsTests
    {
        [Fact]
        public void TestDataPointsCountAndBatchSizeAreEqual_CreatesOnlyOneBatch()
        {
            // Arrange
            var batchSize = 2;
            var metrics = new List<Metric>();
            using var meter = new Meter(Utils.GetCurrentMethodName());
            using var provider = Sdk.CreateMeterProviderBuilder()
                .AddMeter(meter.Name)
                .AddInMemoryExporter(metrics)
                .Build();

            var list = new List<Measurement<int>>(2);
            for (int i = 0; i < list.Capacity; i++)
            {
                var point = new Measurement<int>(i, new KeyValuePair<string, object>(i.ToString(), i));
                list.Add(point);
            }

            meter.CreateObservableGauge("test_gauge", () => list);

            provider.ForceFlush();

            // Act
            var metric = metrics.First();
            var otlpMetrics = metric.ToBatchedOtlpMetric(batchSize);

            // Assert
            Assert.Single(otlpMetrics);
        }

        [Fact]
        public void TestLeftoverDataPoints_AreAlsoBatched()
        {
            // Arrange
            var batchSize = 2;
            var metrics = new List<Metric>();
            using var meter = new Meter(Utils.GetCurrentMethodName());
            using var provider = Sdk.CreateMeterProviderBuilder()
                .AddMeter(meter.Name)
                .AddInMemoryExporter(metrics)
                .Build();

            var list = new List<Measurement<int>>(3);
            for (int i = 0; i < list.Capacity; i++)
            {
                var point = new Measurement<int>(i, new KeyValuePair<string, object>(i.ToString(), i));
                list.Add(point);
            }

            meter.CreateObservableGauge("test_gauge", () => list);

            provider.ForceFlush();
            var metric = metrics.First();

            // Act
            var otlpMetrics = metric.ToBatchedOtlpMetric(batchSize);

            // Assert
            Assert.Equal(batchSize, otlpMetrics.First().Gauge.DataPoints.Count);
            Assert.Single(otlpMetrics.Last().Gauge.DataPoints);
        }
    }
}
