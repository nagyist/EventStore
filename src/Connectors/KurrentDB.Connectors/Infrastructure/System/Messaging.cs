// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

// ReSharper disable CheckNamespace

using DotNext.Collections.Generic;
using KurrentDB.Core.Messaging;

namespace KurrentDB.Core.Bus;

public interface IMessageBus : ISubscriber, IPublisher;

public delegate ValueTask HandleMessageAsync<in T>(T message, CancellationToken cancellationToken) where T : Message;

public delegate void HandleMessage<in T>(T message, CancellationToken cancellationToken) where T : Message;

public delegate void DropMessageSubscription();

public abstract class MessageHandler<T> : IAsyncHandle<T> where T : Message {
    public abstract ValueTask HandleAsync(T message, CancellationToken token);

    public class Proxy(HandleMessageAsync<T> handler) : MessageHandler<T> {
        public override ValueTask HandleAsync(T message, CancellationToken cancellationToken) => handler(message, cancellationToken);
    }
}

[PublicAPI]
public class MessageModule(ISubscriber subscriber) : IDisposable {
    Dictionary<Type, DropMessageSubscription> Subscriptions { get; } = [];

    protected void On<T>(HandleMessageAsync<T> handler) where T : Message {
        var key = typeof(T);

        if (Subscriptions.ContainsKey(key))
            throw new InvalidOperationException($"Already subscribed to {key.Name}");

        var proxy = new MessageHandler<T>.Proxy(handler);
        subscriber.Subscribe(proxy);
        Subscriptions[key] = () => subscriber.Unsubscribe(proxy);
    }

    protected void On<T>(HandleMessage<T> handler) where T : Message => On<T>((msg, token) => {
        handler(msg, token);
        return ValueTask.CompletedTask;
    });

    protected void Drop<T>() where T : Message {
        if (Subscriptions.Remove(typeof(T), out var unsubscribe))
            unsubscribe();
    }

    protected void DropAll() {
        Subscriptions.Values.ForEach(unsubscribe => unsubscribe());
        Subscriptions.Clear();
    }

    public void Dispose() => DropAll();
}

[PublicAPI]
public static class SubscriberExtensions {
    public static DropMessageSubscription On<T>(this ISubscriber subscriber, HandleMessageAsync<T> handler) where T : Message {
        var proxy = new MessageHandler<T>.Proxy(handler);
        subscriber.Subscribe(proxy);
        return () => subscriber.Unsubscribe(proxy);
    }

    public static DropMessageSubscription On<T>(this ISubscriber subscriber, HandleMessage<T> handler) where T : Message =>
        On<T>(subscriber, (msg, token) => {
            handler(msg, token);
            return ValueTask.CompletedTask;
        });
}
