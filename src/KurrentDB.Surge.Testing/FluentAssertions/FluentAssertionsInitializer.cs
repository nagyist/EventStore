// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System.Runtime.CompilerServices;

namespace KurrentDB.Surge.Testing.FluentAssertions;

static class FluentAssertionsInitializer {
	[ModuleInitializer]
	public static void Initialize() =>
		AssertionOptions.AssertEquivalencyUsing(
			options => options
				.Using<ReadOnlyMemory<byte>>(ctx => ctx.Subject.Span.SequenceEqual(ctx.Expectation.Span).Should().BeTrue(ctx.Because, ctx.BecauseArgs))
				.WhenTypeIs<ReadOnlyMemory<byte>>()
				.Using<Memory<byte>>(ctx => ctx.Subject.Span.SequenceEqual(ctx.Expectation.Span).Should().BeTrue(ctx.Because, ctx.BecauseArgs))
				.WhenTypeIs<Memory<byte>>()
		);
}
