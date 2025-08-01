// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System.Reflection;
using Xunit.Sdk;

namespace KurrentDB.Surge.Testing.Xunit.Extensions.AssemblyFixture;

[PublicAPI]
public class XunitTestFrameworkWithAssemblyFixture(IMessageSink messageSink) : XunitTestFramework(messageSink) {
    public const string AssemblyName = "KurrentDB.Surge.Testing";
    public const string TypeName     = $"KurrentDB.Surge.Testing.Xunit.Extensions.AssemblyFixture.{nameof(XunitTestFrameworkWithAssemblyFixture)}";

    protected override ITestFrameworkExecutor CreateExecutor(AssemblyName assemblyName) =>
        new XunitTestFrameworkExecutorWithAssemblyFixture(assemblyName, SourceInformationProvider, DiagnosticMessageSink);
}
