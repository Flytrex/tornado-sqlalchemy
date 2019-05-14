from unittest.mock import Mock

from tornado_sqlalchemy import MissingFactoryError, SessionMixin

from ._common import BaseTestCase, User


class SessionMixinTestCase(BaseTestCase):
    def test_mixin_ok(self):
        class GoodHandler(SessionMixin):
            def __init__(h_self):
                h_self.application = Mock()
                h_self.application.settings = {'session_factory': self.factory}

            def run(h_self):
                with h_self.make_session() as session:
                    return session.query(User).count()

        self.assertEqual(GoodHandler().run(), 0)

    def test_mixin_no_session_factory(self):
        class BadHandler(SessionMixin):
            def __init__(h_self):
                h_self.application = Mock()
                h_self.application.settings = {}

            def run(h_self):
                with h_self.make_session() as session:
                    return session.query(User).count()

        self.assertRaises(MissingFactoryError, BadHandler().run)

    def test_distinct_sessions(self):
        sessions = set()

        class Handler(SessionMixin):
            def __init__(h_self):
                h_self.application = Mock()
                h_self.application.settings = {'session_factory': self.factory}

            def run(h_self):
                session = h_self.session

                sessions.add(id(session))
                value = session.query(User).count()

                session.commit()
                session.close()

                return value

        Handler().run()
        Handler().run()

        self.assertEqual(len(sessions), 2)

    def test_async_session(self):
        class Handler(SessionMixin):
            def __init__(h_self):
                h_self.application = Mock()
                h_self.application.settings = {'session_factory': self.factory}

            async def run(h_self):
                async with h_self.async_make_session() as session:
                    new_user1 = User(username="testusername1")
                    await h_self.run_in_executor(session.add, new_user)

                    new_user2 = User(username="testusername2")
                    await h_self.run_in_executor(session.add, new_user)


                async with h_self.async_make_session() as session:
                    count = h_self.run_in_executor(session.query(User).count)
                    self.assertEqual(count, 2)

        Handler().run()


