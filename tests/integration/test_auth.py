import requests

def test_auth(relay, mini_sentry):
    r1 = relay(mini_sentry)
    r1.wait_authenticated()
    r2 = relay(r1)
    r2.wait_authenticated()

    assert requests.get(r2.url + '/test').text == 'ok'
