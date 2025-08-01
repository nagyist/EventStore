// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

namespace Kurrent.Surge.Schema.Validation;

/// <summary>
/// Defines a manager responsible for checking the compatibility between schemas.
/// </summary>
public interface ISchemaCompatibilityManager {
    /// <summary>
    /// Asynchronously checks the compatibility between two schema definitions.
    /// </summary>
    /// <param name="uncheckedSchema">The string representation of the schema to be validated.</param>
    /// <param name="referenceSchema">The string representation of the reference schema to validate against.</param>
    /// <param name="compatibility">The compatibility mode to enforce (e.g. Backward, Forward, Full).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
    /// <returns>
    /// A <see cref="ValueTask{TResult}"/> that represents the asynchronous operation.
    /// The task result contains a <see cref="SchemaCompatibilityResult"/> indicating whether the schemas are compatible,
    /// according to the specified mode, potentially including details about incompatibilities.
    /// </returns>
    ValueTask<SchemaCompatibilityResult> CheckCompatibility(
        string uncheckedSchema,
        string referenceSchema,
        SchemaCompatibilityMode compatibility,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Asynchronously checks the compatibility between schema definitions.
    /// </summary>
    /// <param name="uncheckedSchema">The string representation of the schema to be validated.</param>
    /// <param name="referenceSchemas">The string representation(s) of the reference schema(s) to validate against. This can be a single schema or multiple schemas for *All compatibility modes.</param>
    /// <param name="compatibility">The compatibility mode to enforce (e.g. Backward, Forward, Full, BackwardAll, ForwardAll, FullAll).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
    /// <returns>
    /// A <see cref="ValueTask{TResult}"/> that represents the asynchronous operation.
    /// The task result contains a <see cref="SchemaCompatibilityResult"/> indicating whether the schemas are compatible,
    /// according to the specified mode, potentially including details about incompatibilities.
    /// </returns>
    ValueTask<SchemaCompatibilityResult> CheckCompatibility(
        string uncheckedSchema,
        IEnumerable<string> referenceSchemas,
        SchemaCompatibilityMode compatibility,
        CancellationToken cancellationToken = default
    );
}

public abstract class SchemaCompatibilityManagerBase : ISchemaCompatibilityManager {
    /// <inheritdoc />
    public ValueTask<SchemaCompatibilityResult> CheckCompatibility(
        string uncheckedSchema,
        string referenceSchema,
        SchemaCompatibilityMode compatibility,
        CancellationToken cancellationToken = default
    ) => CheckCompatibility(uncheckedSchema, [referenceSchema], compatibility, cancellationToken);

    /// <inheritdoc />
    public ValueTask<SchemaCompatibilityResult> CheckCompatibility(
        string uncheckedSchema,
        IEnumerable<string> referenceSchemas,
        SchemaCompatibilityMode compatibility,
        CancellationToken cancellationToken = default
    ) {
        Ensure.IsDefined(compatibility);

        if (compatibility is SchemaCompatibilityMode.None)
            return ValueTask.FromResult(SchemaCompatibilityResult.Compatible());

        try {
	        return CheckCompatibilityCore(uncheckedSchema, referenceSchemas.ToArray(), compatibility, cancellationToken);
        } catch (Exception ex) {
	        throw new CompatibilityManagerException($"Error checking schema compatibility: {ex.Message}", ex);
        }
    }

    protected abstract ValueTask<SchemaCompatibilityResult> CheckCompatibilityCore(
        string uncheckedSchema,
        string[] referenceSchemas,
        SchemaCompatibilityMode compatibility,
        CancellationToken cancellationToken = default
    );
}

/// <summary>
/// Represents an exception that occurs during schema compatibility checks within the compatibility manager.
/// </summary>
public class CompatibilityManagerException(string message, Exception? innerException) : Exception(message, innerException);
