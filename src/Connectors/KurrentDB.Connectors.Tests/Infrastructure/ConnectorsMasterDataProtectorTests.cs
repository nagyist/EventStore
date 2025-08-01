// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using Kurrent.Surge.DataProtection;
using KurrentDB.Common.Configuration;
using KurrentDB.Connectors.Infrastructure;
using KurrentDB.Connectors.Infrastructure.Connect.Components.Connectors;

namespace KurrentDB.Connectors.Tests.Infrastructure;

public class ConnectorsMasterDataProtectorTests(ITestOutputHelper output, ConnectorsAssemblyFixture fixture) : ConnectorsIntegrationTests(output, fixture) {
    [Fact]
    public void protects_sensitive_data_when_using_valid_token() {
        // Arrange
        IConnectorDataProtector sut = new ConnectorsMasterDataProtector(
            Fixture.DataProtector,
            new DataProtectionOptions { Token = "SOME-VALID-TOKEN" }
        );

        var settings = new Dictionary<string, string?> {
            ["instanceTypeName"]              = "http-sink",
            ["authentication:basic:username"] = "tim",
            ["authentication:basic:password"] = "secret"
        };

        // Act
        var protectedSettings = sut.Protect("connectorId", settings.ToDictionary());

        // Assert
        protectedSettings.Should().NotBeEquivalentTo(settings);
    }

    [Fact]
    public void fails_to_protect_sensitive_data_when_using_noop_token() {
        // Arrange
        IConnectorDataProtector sut = new ConnectorsMasterDataProtector(
            Fixture.DataProtector,
            new DataProtectionOptions { Token = DataProtectionConstants.NoOpToken }
        );

        var settings = new Dictionary<string, string?> {
            ["instanceTypeName"]              = "http-sink",
            ["authentication:basic:username"] = "tim",
            ["authentication:basic:password"] = "secret"
        };

        // Act
        var protect = () => sut.Protect("connectorId", settings);

        // Assert
        protect.Should()
            .Throw<DataProtectionException>()
            .WithMessage("Data protection token not found!*");
    }

    [Fact]
    public void unprotects_sensitive_data_when_using_valid_token() {
        // Arrange
        IConnectorDataProtector sut = new ConnectorsMasterDataProtector(
            Fixture.DataProtector,
            new DataProtectionOptions { Token = "SOME-VALID-TOKEN" }
        );

        var settings = new Dictionary<string, string?> {
            ["instanceTypeName"]              = "http-sink",
            ["authentication:basic:username"] = "tim",
            ["authentication:basic:password"] = "secret"
        };

        var protectedConfiguration = sut.Protect("connectorId", settings.ToDictionary()).ToConfiguration();

        // Act
        var unprotectedSettings = sut.Unprotect(protectedConfiguration).ToSettings();

        // Assert
        unprotectedSettings.Should().BeEquivalentTo(unprotectedSettings);
    }

    [Fact]
    public void unprotect_returns_original_data_when_using_noop_token() {
        // Arrange
        IConnectorDataProtector validProtector = new ConnectorsMasterDataProtector(
            Fixture.DataProtector,
            new DataProtectionOptions { Token = "SOME-VALID-TOKEN" }
        );

        IConnectorDataProtector noOpProtector = new ConnectorsMasterDataProtector(
            Fixture.DataProtector,
            new DataProtectionOptions { Token = DataProtectionConstants.NoOpToken }
        );

        var settings = new Dictionary<string, string?> {
            ["instanceTypeName"]              = "http-sink",
            ["authentication:basic:username"] = "tim",
            ["authentication:basic:password"] = "secret"
        };

        var protectedSettings = validProtector.Protect("connectorId", settings.ToDictionary());

        // Act
        var unprotectedSettings = noOpProtector.Unprotect(protectedSettings.ToConfiguration()).ToSettings();

        // Assert
        unprotectedSettings.Should().BeEquivalentTo(protectedSettings);
    }
}