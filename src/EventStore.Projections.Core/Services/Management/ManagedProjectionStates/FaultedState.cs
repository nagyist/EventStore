// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

namespace EventStore.Projections.Core.Services.Management.ManagedProjectionStates;

class FaultedState : ManagedProjectionStateBase {
	public FaultedState(ManagedProjection managedProjection)
		: base(managedProjection) {
	}
}
