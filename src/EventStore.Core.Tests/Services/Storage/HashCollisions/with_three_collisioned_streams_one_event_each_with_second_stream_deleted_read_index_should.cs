// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using NUnit.Framework;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.HashCollisions;

[TestFixture(typeof(LogFormat.V2), typeof(string))]
[TestFixture(typeof(LogFormat.V3), typeof(uint))]
public class
	with_three_collisioned_streams_one_event_each_with_second_stream_deleted_read_index_should<TLogFormat, TStreamId> :
		ReadIndexTestScenario<TLogFormat, TStreamId> {
	private EventRecord _prepare1;
	private EventRecord _prepare2;
	private EventRecord _delete2;
	private EventRecord _prepare3;

	protected override async ValueTask WriteTestScenario(CancellationToken token) {
		_prepare1 = await WriteSingleEvent("AB", 0, "test1", token: token);

		_prepare2 = await WriteSingleEvent("CD", 0, "test2", token: token);
		_delete2 = await WriteDelete("CD", token);

		_prepare3 = await WriteSingleEvent("EF", 0, "test3", token: token);
	}

	[Test]
	public async Task return_correct_last_event_version_for_first_stream() {
		Assert.AreEqual(0, await ReadIndex.GetStreamLastEventNumber("AB", CancellationToken.None));
	}

	[Test]
	public async Task return_correct_log_record_for_first_stream() {
		var result = await ReadIndex.ReadEvent("AB", 0, CancellationToken.None);
		Assert.AreEqual(ReadEventResult.Success, result.Result);
		Assert.AreEqual(_prepare1, result.Record);
	}

	[Test]
	public async Task not_find_record_with_index_1_in_first_stream() {
		var result = await ReadIndex.ReadEvent("AB", 1, CancellationToken.None);
		Assert.AreEqual(ReadEventResult.NotFound, result.Result);
		Assert.IsNull(result.Record);
	}

	[Test]
	public async Task return_correct_range_on_from_start_range_query_for_first_stream() {
		var result = await ReadIndex.ReadStreamEventsForward("AB", 0, 1, CancellationToken.None);
		Assert.AreEqual(ReadStreamResult.Success, result.Result);
		Assert.AreEqual(1, result.Records.Length);
		Assert.AreEqual(_prepare1, result.Records[0]);
	}

	[Test]
	public async Task return_correct_range_on_from_end_range_query_for_first_stream() {
		var result = await ReadIndex.ReadStreamEventsBackward("AB", 0, 1, CancellationToken.None);
		Assert.AreEqual(ReadStreamResult.Success, result.Result);
		Assert.AreEqual(1, result.Records.Length);
		Assert.AreEqual(_prepare1, result.Records[0]);
	}

	[Test]
	public async Task return_correct_last_event_version_for_second_stream() {
		Assert.AreEqual(EventNumber.DeletedStream, await ReadIndex.GetStreamLastEventNumber("CD", CancellationToken.None));
	}

	[Test]
	public async Task not_find_log_record_for_second_stream() {
		var result = await ReadIndex.ReadEvent("CD", 0, CancellationToken.None);
		Assert.AreEqual(ReadEventResult.StreamDeleted, result.Result);
		Assert.IsNull(result.Record);
	}

	[Test]
	public async Task not_find_record_with_index_1_in_second_stream() {
		var result = await ReadIndex.ReadEvent("CD", 1, CancellationToken.None);
		Assert.AreEqual(ReadEventResult.StreamDeleted, result.Result);
		Assert.IsNull(result.Record);
	}

	[Test]
	public async Task return_empty_range_on_from_start_range_query_for_second_stream() {
		var result = await ReadIndex.ReadStreamEventsForward("CD", 0, 1, CancellationToken.None);
		Assert.AreEqual(ReadStreamResult.StreamDeleted, result.Result);
	}

	[Test]
	public async Task return_empty_range_on_from_end_range_query_for_second_stream() {
		var result = await ReadIndex.ReadStreamEventsBackward("CD", 0, 1, CancellationToken.None);
		Assert.AreEqual(ReadStreamResult.StreamDeleted, result.Result);
	}

	[Test]
	public async Task return_correct_last_event_version_for_third_stream() {
		Assert.AreEqual(0, await ReadIndex.GetStreamLastEventNumber("EF", CancellationToken.None));
	}

	[Test]
	public async Task return_correct_log_record_for_third_stream() {
		var result = await ReadIndex.ReadEvent("EF", 0, CancellationToken.None);
		Assert.AreEqual(ReadEventResult.Success, result.Result);
		Assert.AreEqual(_prepare3, result.Record);
	}

	[Test]
	public async Task not_find_record_with_index_1_in_third_stream() {
		var result = await ReadIndex.ReadEvent("EF", 1, CancellationToken.None);
		Assert.AreEqual(ReadEventResult.NotFound, result.Result);
		Assert.IsNull(result.Record);
	}

	[Test]
	public async Task return_correct_range_on_from_start_range_query_for_third_stream() {
		var result = await ReadIndex.ReadStreamEventsForward("EF", 0, 1, CancellationToken.None);
		Assert.AreEqual(ReadStreamResult.Success, result.Result);
		Assert.AreEqual(1, result.Records.Length);
		Assert.AreEqual(_prepare3, result.Records[0]);
	}

	[Test]
	public async Task return_correct_range_on_from_end_range_query_for_third_stream() {
		var result = await ReadIndex.ReadStreamEventsBackward("EF", 0, 1, CancellationToken.None);
		Assert.AreEqual(ReadStreamResult.Success, result.Result);
		Assert.AreEqual(1, result.Records.Length);
		Assert.AreEqual(_prepare3, result.Records[0]);
	}

	[Test]
	public async Task return_empty_range_when_asked_to_get_few_events_from_start_starting_from_1_in_first_stream() {
		var result = await ReadIndex.ReadStreamEventsForward("AB", 1, 1, CancellationToken.None);
		Assert.AreEqual(ReadStreamResult.Success, result.Result);
		Assert.AreEqual(0, result.Records.Length);
	}

	[Test]
	public async Task return_empty_range_when_asked_to_get_few_events_from_start_starting_from_1_in_second_stream() {
		var result = await ReadIndex.ReadStreamEventsForward("CD", 1, 1, CancellationToken.None);
		Assert.AreEqual(ReadStreamResult.StreamDeleted, result.Result);
		Assert.AreEqual(0, result.Records.Length);
	}

	[Test]
	public async Task return_empty_range_when_asked_to_get_few_events_from_start_starting_from_1_in_third_stream() {
		var result = await ReadIndex.ReadStreamEventsForward("EF", 1, 1, CancellationToken.None);
		Assert.AreEqual(ReadStreamResult.Success, result.Result);
		Assert.AreEqual(0, result.Records.Length);
	}

	[Test]
	public async Task return_empty_range_when_asked_to_get_few_events_from_end_starting_from_1_in_first_stream() {
		var result = await ReadIndex.ReadStreamEventsBackward("AB", 1, 1, CancellationToken.None);
		Assert.AreEqual(ReadStreamResult.Success, result.Result);
		Assert.AreEqual(0, result.Records.Length);
	}

	[Test]
	public async Task return_empty_range_when_asked_to_get_few_events_from_end_starting_from_1_in_second_stream() {
		var result = await ReadIndex.ReadStreamEventsBackward("CD", 1, 1, CancellationToken.None);
		Assert.AreEqual(ReadStreamResult.StreamDeleted, result.Result);
		Assert.AreEqual(0, result.Records.Length);
	}

	[Test]
	public async Task return_empty_range_when_asked_to_get_few_events_from_end_starting_from_1_in_third_stream() {
		var result = await ReadIndex.ReadStreamEventsBackward("EF", 1, 1, CancellationToken.None);
		Assert.AreEqual(ReadStreamResult.Success, result.Result);
		Assert.AreEqual(0, result.Records.Length);
	}

	[Test]
	public async Task return_correct_last_event_version_for_nonexistent_stream_with_same_hash() {
		Assert.AreEqual(-1, await ReadIndex.GetStreamLastEventNumber("ZZ", CancellationToken.None));
	}

	[Test]
	public async Task not_find_log_record_for_nonexistent_stream_with_same_hash() {
		var result = await ReadIndex.ReadEvent("ZZ", 0, CancellationToken.None);
		Assert.AreEqual(ReadEventResult.NoStream, result.Result);
		Assert.IsNull(result.Record);
	}

	[Test]
	public async Task return_empty_range_on_from_start_range_query_for_non_existing_stream_with_same_hash() {
		var result = await ReadIndex.ReadStreamEventsForward("ZZ", 0, 1, CancellationToken.None);
		Assert.AreEqual(ReadStreamResult.NoStream, result.Result);
		Assert.AreEqual(0, result.Records.Length);
	}

	[Test]
	public async Task return_empty_range_on_from_end_range_query_for_non_existing_stream_with_same_hash() {
		var result = await ReadIndex.ReadStreamEventsBackward("ZZ", 0, 1, CancellationToken.None);
		Assert.AreEqual(ReadStreamResult.NoStream, result.Result);
		Assert.AreEqual(0, result.Records.Length);
	}

	[Test]
	public async Task return_all_events_on_read_all_forward() {
		var events = (await ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 100, CancellationToken.None))
			.EventRecords()
			.Select(r => r.Event)
			.ToArray();
		Assert.AreEqual(4, events.Length);
		Assert.AreEqual(_prepare1, events[0]);
		Assert.AreEqual(_prepare2, events[1]);
		Assert.AreEqual(_delete2, events[2]);
		Assert.AreEqual(_prepare3, events[3]);
	}

	[Test]
	public async Task return_all_events_on_read_all_backward() {
		var events = (await ReadIndex.ReadAllEventsBackward(GetBackwardReadPos(), 100, CancellationToken.None))
			.EventRecords()
			.Select(r => r.Event)
			.ToArray();
		Assert.AreEqual(4, events.Length);
		Assert.AreEqual(_prepare1, events[3]);
		Assert.AreEqual(_prepare2, events[2]);
		Assert.AreEqual(_delete2, events[1]);
		Assert.AreEqual(_prepare3, events[0]);
	}
}
