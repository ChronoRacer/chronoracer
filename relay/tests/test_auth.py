import threading
from types import SimpleNamespace

import pytest

from app.auth import okta_user, pubsub_push


@pytest.mark.asyncio
@pytest.mark.parametrize('push', [False, True])
async def test_jwks_validation_does_not_block_event_loop(push):
    loop_thread = threading.get_ident()
    validation_threads = []
    class Authenticator:
        def validate_token(self, token):
            assert token == 'access-token'
            validation_threads.append(threading.get_ident())
            return {'sub': 'operator'}

        validate = validate_token

    authenticator = Authenticator()
    request = SimpleNamespace(
        headers={'authorization': 'Bearer access-token'},
        app=SimpleNamespace(state=SimpleNamespace(
            okta=authenticator, pubsub_auth={('audience', 'push@example.test'): authenticator},
        )),
    )
    if push:
        assert await pubsub_push(request, 'audience', 'push@example.test') == {'sub': 'operator'}
    else:
        assert (await okta_user(request)).sub == 'operator'
    assert len(validation_threads) == 1
    assert validation_threads[0] != loop_thread
