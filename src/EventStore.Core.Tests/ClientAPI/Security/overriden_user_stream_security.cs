﻿using System;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Services;
using NUnit.Framework;

namespace EventStore.Core.Tests.ClientAPI.Security
{
    [TestFixture, Category("LongRunning"), Category("Network")]
    public class overriden_user_stream_security : AuthenticationTestBase
    {
        [TestFixtureSetUp]
        public override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();
            
            var settings = new SystemSettings(userStreamAcl: new StreamAcl("user1", "user1", "user1", "user1", "user1"),
                                              systemStreamAcl: null);
            Connection.SetSystemSettingsAsync(settings, new UserCredentials("adm", "admpa$$")).Wait();
        }

        [Test, Category("LongRunning"), Category("Network")]
        public void operations_on_user_stream_succeeds_for_authorized_user()
        {
            const string stream = "user-authorized-user";
            ExpectNoException(() => ReadEvent(stream, "user1", "pa$$1"));
            ExpectNoException(() => ReadStreamForward(stream, "user1", "pa$$1"));
            ExpectNoException(() => ReadStreamBackward(stream, "user1", "pa$$1"));

            ExpectNoException(() => WriteStream(stream, "user1", "pa$$1"));
            ExpectNoException(() => TransStart(stream, "user1", "pa$$1"));
            {
                var transId = TransStart(stream, "adm", "admpa$$").TransactionId;
                var trans = Connection.ContinueTransaction(transId, new UserCredentials("user1", "pa$$1"));
                ExpectNoException(() => trans.Write());
                ExpectNoException(() => trans.Commit());
            };

            ExpectNoException(() => ReadMeta(stream, "user1", "pa$$1"));
            ExpectNoException(() => WriteMeta(stream, "user1", "pa$$1", null));

            ExpectNoException(() => SubscribeToStream(stream, "user1", "pa$$1"));

            ExpectNoException(() => DeleteStream(stream, "user1", "pa$$1"));
        }

        [Test, Category("LongRunning"), Category("Network")]
        public void operations_on_user_stream_fail_for_not_authorized_user()
        {
            const string stream = "user-not-authorized";
            Expect<AccessDeniedException>(() => ReadEvent(stream, "user2", "pa$$2"));
            Expect<AccessDeniedException>(() => ReadStreamForward(stream, "user2", "pa$$2"));
            Expect<AccessDeniedException>(() => ReadStreamBackward(stream, "user2", "pa$$2"));

            Expect<AccessDeniedException>(() => WriteStream(stream, "user2", "pa$$2"));
            Expect<AccessDeniedException>(() => TransStart(stream, "user2", "pa$$2"));
            {
                var transId = TransStart(stream, "adm", "admpa$$").TransactionId;
                var trans = Connection.ContinueTransaction(transId, new UserCredentials("user2", "pa$$2"));
                ExpectNoException(() => trans.Write());
                Expect<AccessDeniedException>(() => trans.Commit());
            };

            Expect<AccessDeniedException>(() => ReadMeta(stream, "user2", "pa$$2"));
            Expect<AccessDeniedException>(() => WriteMeta(stream, "user2", "pa$$2", null));

            Expect<AccessDeniedException>(() => SubscribeToStream(stream, "user2", "pa$$2"));

            Expect<AccessDeniedException>(() => DeleteStream(stream, "user2", "pa$$2"));
        }

        [Test, Category("LongRunning"), Category("Network")]
        public void operations_on_user_stream_fail_for_anonymous_user()
        {
            const string stream = "user-anonymous-user";
            Expect<AccessDeniedException>(() => ReadEvent(stream, null, null));
            Expect<AccessDeniedException>(() => ReadStreamForward(stream, null, null));
            Expect<AccessDeniedException>(() => ReadStreamBackward(stream, null, null));

            Expect<AccessDeniedException>(() => WriteStream(stream, null, null));
            Expect<AccessDeniedException>(() => TransStart(stream, null, null));
            {
                var transId = TransStart(stream, "adm", "admpa$$").TransactionId;
                var trans = Connection.ContinueTransaction(transId);
                ExpectNoException(() => trans.Write());
                Expect<AccessDeniedException>(() => trans.Commit());
            };

            Expect<AccessDeniedException>(() => ReadMeta(stream, null, null));
            Expect<AccessDeniedException>(() => WriteMeta(stream, null, null, null));

            Expect<AccessDeniedException>(() => SubscribeToStream(stream, null, null));

            Expect<AccessDeniedException>(() => DeleteStream(stream, null, null));
        }

        [Test, Category("LongRunning"), Category("Network")]
        public void operations_on_user_stream_succeed_for_admin()
        {
            const string stream = "user-admin";
            ExpectNoException(() => ReadEvent(stream, "adm", "admpa$$"));
            ExpectNoException(() => ReadStreamForward(stream, "adm", "admpa$$"));
            ExpectNoException(() => ReadStreamBackward(stream, "adm", "admpa$$"));

            ExpectNoException(() => WriteStream(stream, "adm", "admpa$$"));
            ExpectNoException(() => TransStart(stream, "adm", "admpa$$"));
            {
                var transId = TransStart(stream, "adm", "admpa$$").TransactionId;
                var trans = Connection.ContinueTransaction(transId, new UserCredentials("adm", "admpa$$"));
                ExpectNoException(() => trans.Write());
                ExpectNoException(() => trans.Commit());
            };

            ExpectNoException(() => ReadMeta(stream, "adm", "admpa$$"));
            ExpectNoException(() => WriteMeta(stream, "adm", "admpa$$", null));

            ExpectNoException(() => SubscribeToStream(stream, "adm", "admpa$$"));

            ExpectNoException(() => DeleteStream(stream, "adm", "admpa$$"));
        }
    }
}