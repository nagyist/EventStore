// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

// ReSharper disable MethodSupportsCancellation

using System.Text.RegularExpressions;
using Kurrent.Surge;
using Kurrent.Surge.Consumers;
using KurrentDB.Connect.Consumers;
using KurrentDB.Core;
using KurrentDB.Core.Services.Transport.Enumerators;
using KurrentDB.Surge.Testing.Fixtures;
using KurrentDB.Surge.Testing.Xunit;
using Microsoft.Extensions.Logging;
using Identifiers = Kurrent.Surge.Identifiers;

namespace KurrentDB.Surge.Tests.Components.Consumers;

[Trait("Category", "Integration")]
public class SystemConsumerTests(ITestOutputHelper output, SystemComponentsAssemblyFixture fixture) : SystemComponentsIntegrationTests(output, fixture) {
	[Theory, ConsumeFilterCases]
	public async Task consumes_stream_from_earliest(string streamId, ConsumeFilter filter) {
		// Arrange
		var requests = await Fixture.ProduceTestEvents(streamId, 1, 10);
		var messages = requests.SelectMany(x => x.Messages).ToList();

		using var cancellator = new CancellationTokenSource(TimeSpan.FromSeconds(360));

		var pendingCount = messages.Count;

		var consumedRecords = new List<SurgeRecord>();

		await using var consumer = Fixture.NewConsumer()
			.ConsumerId($"{streamId}-csr")
			.Filter(filter)
			.InitialPosition(SubscriptionInitialPosition.Earliest)
			.DisableAutoCommit()
			.Create();

		// Act
		await foreach (var record in consumer.Records(cancellator.Token)) {
            if (record.Value is ReadResponse.CheckpointReceived or ReadResponse.SubscriptionCaughtUp)
                continue;

			pendingCount--;
			consumedRecords.Add(record);

			if (pendingCount == 0)
				await cancellator.CancelAsync();
		}

		// Assert
		consumedRecords.Should()
			.HaveCount(messages.Count, "because we consumed all the records in the stream");

		var actualEvents = await Fixture.Publisher.ReadFullStream(streamId).ToListAsync();

		var actualRecords = await Task.WhenAll(
			actualEvents.Select((re, idx) => re.ToRecord((data, headers) => Fixture.SchemaSerializer.Deserialize(data, new(headers)), idx + 1).AsTask()).ToArray()
		);

        consumedRecords
            .Should()
            .BeEquivalentTo(actualRecords,
                options => options.WithStrictOrderingFor(x => x.Position).Excluding(record => record.SequenceId),
                "because we consumed all the records in the stream");
	}

	[Theory, ConsumeFilterCases]
    public Task consumes_stream_from_latest(string streamId, ConsumeFilter filter) => Fixture.TestWithTimeout(
        async cancellator => {
            // Arrange
            await Fixture.ProduceTestEvents(streamId);

            await using var consumer = Fixture.NewConsumer()
                .ConsumerId($"{streamId}-csr")
                .Filter(filter)
                .InitialPosition(SubscriptionInitialPosition.Latest)
                .DisableAutoCommit()
                .Create();

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            using var linked  = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token, cancellator.Token);

            // Act
            var consumed = await consumer
                .Records(cancellator.Token)
                .Where(x => x.Value is not (ReadResponse.CheckpointReceived or ReadResponse.SubscriptionCaughtUp))
                .ToListAsync(linked.Token);

            // Assert
            consumed.Should().BeEmpty("because there are no records in the stream");
        }
    );

	[Theory, ConsumeFilterCases]
	public async Task consumes_stream_from_start_position(string streamId, ConsumeFilter filter) {
		// Arrange
        var noise         = await Fixture.ProduceTestEvents(streamId);
        var startPosition = noise.Single().Position;
        var requests      = await Fixture.ProduceTestEvents(streamId);
        var messages      = requests.SelectMany(x => x.Messages).ToList();

		using var cancellator = new CancellationTokenSource(TimeSpan.FromMinutes(1));

		var consumedRecords = new List<SurgeRecord>();

		await using var consumer = Fixture.NewConsumer()
			.ConsumerId($"{streamId}-csr")
			.Filter(filter)
			.StartPosition(startPosition)
			.DisableAutoCommit()
			.Create();

		// Act
		await foreach (var record in consumer.Records(cancellator.Token)) {
            if (record.Value is ReadResponse.CheckpointReceived or ReadResponse.SubscriptionCaughtUp)
                continue;

			messages.Should().Contain(x => x.RecordId == record.Id);

			consumedRecords.Add(record);
			await consumer.Track(record);

			if (consumedRecords.Count == messages.Count)
				await cancellator.CancelAsync();
		}

		await consumer.CommitAll();

		// Assert
		consumedRecords.All(x => x.StreamId == streamId).Should().BeTrue();
		consumedRecords.Should().HaveCount(messages.Count);

		var positions = await consumer.GetLatestPositions();

		positions.Last().LogPosition.Should().BeEquivalentTo(consumedRecords.Last().LogPosition);
	}

	async Task<RecordPosition> ProduceAndConsumeTestStream(
        string streamId, ConsumeFilter filter, int numberOfMessages, CancellationToken cancellationToken, bool commit = true
    ) {
		using var cancellator = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var requests        = await Fixture.ProduceTestEvents(streamId, numberOfRequests: 1, numberOfMessages);
        var messageCount    = requests.SelectMany(x => x.Messages).Count();
        var consumedRecords = new List<SurgeRecord>();

		await using var consumer = Fixture.NewConsumer()
			.ConsumerId($"{streamId}-csr")
			.SubscriptionName($"{streamId}-csr")
			.Filter(filter)
			.InitialPosition(SubscriptionInitialPosition.Earliest)
			.DisableAutoCommit()
			.Create();

		await foreach (var record in consumer.Records(cancellator.Token)) {
            if (record.Value is ReadResponse.CheckpointReceived or ReadResponse.SubscriptionCaughtUp)
                continue;

			consumedRecords.Add(record);
			await consumer.Track(record);

			if (consumedRecords.Count == messageCount)
				await cancellator.CancelAsync();
		}

        if (commit)
		    await consumer.CommitAll();

		var latestPositions = await consumer.GetLatestPositions(CancellationToken.None);

		return latestPositions.LastOrDefault();

		// return consumedRecords.Last().Position;
	}

	[Theory, ConsumeFilterCases]
	public async Task consumes_stream_from_last_committed_position(string streamId, ConsumeFilter filter) {
		// Arrange
		using var cancellator = new CancellationTokenSource(TimeSpan.FromSeconds(720));

		var latestRecordPosition = await ProduceAndConsumeTestStream(streamId, filter, 10, cancellator.Token, commit: false);

		var requests        = await Fixture.ProduceTestEvents(streamId, 1, 1);
		var messages        = requests.SelectMany(x => x.Messages).ToList();
		var consumedRecords = new List<SurgeRecord>();

		await using var consumer = Fixture.NewConsumer()
			.ConsumerId($"{streamId}-csr")
			.SubscriptionName($"{streamId}-csr")
			.Filter(filter)
            .StartPosition(latestRecordPosition.LogPosition)
            .DisableAutoCommit()
			.Create();

		// Act
		await foreach (var record in consumer.Records(cancellator.Token)) {
            if (record.Value is ReadResponse.CheckpointReceived or ReadResponse.SubscriptionCaughtUp)
                continue;

			consumedRecords.Add(record);
			await consumer.Track(record);

			if (consumedRecords.Count == messages.Count)
				await cancellator.CancelAsync();
		}

		await consumer.CommitAll();

		// Assert
		consumedRecords.All(x => x.StreamId == streamId).Should().BeTrue();
		consumedRecords.Should().HaveCount(messages.Count);
	}

	[Theory, ConsumeFilterCases]
	public async Task consumes_stream_and_commits_positions_on_dispose(string streamId, ConsumeFilter filter) {
		// Arrange
		var requests = await Fixture.ProduceTestEvents(streamId, 1, 10);
		var messages = requests.SelectMany(x => x.Messages).ToList();

		using var cancellator = new CancellationTokenSource(TimeSpan.FromSeconds(360));

		var consumedRecords = new List<SurgeRecord>();

		var consumer = Fixture.NewConsumer()
			.ConsumerId($"{streamId}-csr")
			.Filter(filter)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
			.AutoCommit(x => x with { RecordsThreshold = 1 })
			.Create();

		// Act
		await foreach (var record in consumer.Records(cancellator.Token)) {
            if (record.Value is ReadResponse.CheckpointReceived or ReadResponse.SubscriptionCaughtUp)
                continue;

			consumedRecords.Add(record);
			await consumer.Track(record);

			if (consumedRecords.Count == messages.Count)
				await cancellator.CancelAsync();
		}

		// Assert
		consumedRecords.Should()
			.HaveCount(messages.Count, "because we consumed all the records in the stream");

		await consumer.DisposeAsync();

		var latestPositions = await consumer.GetLatestPositions(CancellationToken.None);

		latestPositions.LastOrDefault().LogPosition.Should()
			.BeEquivalentTo(consumedRecords.LastOrDefault().LogPosition);
	}

    class ConsumeFilterCases : TestCaseGeneratorXunit<ConsumeFilterCases> {
        protected override IEnumerable<object[]> Data() {
	        var streamId = Identifiers.GenerateShortId("stream");
            yield return [streamId, ConsumeFilter.FromStreamId(streamId)];

            streamId = Guid.NewGuid().ToString();
            yield return [streamId, ConsumeFilter.FromRegex(ConsumeFilterScope.Stream, new Regex(streamId))];
        }
    }

    [Fact]
    public async Task consumes_all_stream_and_handles_caught_up() {
	    // Arrange
	    // var streamId = Identifiers.GenerateShortId("stream");

	    var filter = ConsumeFilter.FromRegex(ConsumeFilterScope.Stream, new Regex(Identifiers.GenerateShortId()));

	    var requests = await Fixture.ProduceTestEvents(Identifiers.GenerateShortId("stream"), 1, 1000);

	    using var cancellator = new CancellationTokenSource(TimeSpan.FromSeconds(30));

	    var consumedRecords = new List<SurgeRecord>();

	    await using var consumer = Fixture.NewConsumer()
		    // .ConsumerId($"{streamId}-csr")
		    .Filter(filter)
		    .InitialPosition(SubscriptionInitialPosition.Earliest)
		    .DisableAutoCommit()
		    .AutoCommit(options => options with { RecordsThreshold = 100 })
		    .Create();

	    // Act
	    await foreach (var record in consumer.Records(cancellator.Token)) {
		    if (record.Value is ReadResponse.CheckpointReceived) {
			    consumedRecords.Add(record);
			    Fixture.Logger.LogInformation("Checkpoint received at position: {Position}", record.Position);

			    // if (consumedRecords.Count != 10)
				   //  continue;
			    //
			    // await cancellator.CancelAsync();
			    // Fixture.Logger.LogInformation("Cancelling subscription after 10 CheckpointReceived events");
		    }
		    else if (record.Value is ReadResponse.SubscriptionCaughtUp) {
			    await cancellator.CancelAsync();
			    Fixture.Logger.LogInformation("Subscription caught up at position: {Position}", record.Position);
		    }
	    }

	    // Assert
	    consumedRecords.Should()
		    .HaveCountGreaterOrEqualTo(1, "because we should have received at least 10 CheckpointReceived events");
    }
}
