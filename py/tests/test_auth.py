import uuid
import smith
import pytest


def test_basic_key_functions():
    sk, pk = smith.generate_key_pair()
    signature = sk.sign(b'some secret data')
    assert pk.verify(b'some secret data', signature)
    assert not pk.verify(b'some other data', signature)


def test_challenge_response():
    resp = smith.create_register_challenge(b'{"timestamp":"2018-02-28T16:10:13.301546Z","data":{"relay_id":"88a4182a-03f2-42b5-a3a6-024a3ad78a2a","public_key":"GJpPsFJx70jY20wslkT-pDHplJtsv7oZ4sS6fSVaIoQ"}}', 'SXmBYCCsq0iNxm_IkWOJ1eVbMlbKz5Bx-_7T3iRhVvPz1Fq0ifP0F6OlgvVDZX4bxN3w-dEC7l3m307hGoyQBA', max_age=0xffffffff)
    assert str(resp['public_key']) == 'GJpPsFJx70jY20wslkT-pDHplJtsv7oZ4sS6fSVaIoQ'
    assert resp['relay_id'] == uuid.UUID('88a4182a-03f2-42b5-a3a6-024a3ad78a2a')
    assert len(resp['token']) > 40


def test_challenge_response_validation_errors():
    with pytest.raises(smith.UnpackErrorSignatureExpired):
        smith.create_register_challenge(b'{"timestamp":"2018-02-28T16:10:13.301546Z","data":{"relay_id":"88a4182a-03f2-42b5-a3a6-024a3ad78a2a","public_key":"GJpPsFJx70jY20wslkT-pDHplJtsv7oZ4sS6fSVaIoQ"}}', 'SXmBYCCsq0iNxm_IkWOJ1eVbMlbKz5Bx-_7T3iRhVvPz1Fq0ifP0F6OlgvVDZX4bxN3w-dEC7l3m307hGoyQBA', max_age=1)
    with pytest.raises(smith.UnpackErrorBadPayload):
        smith.create_register_challenge(b'{"timestamp":"2018-02-28T16:10:13.301546Z","data":{"relay_id":"88a4182a-03f2-42b5-a3a6-024a3ad78a2a","public_key":"GJpPsFJx70jY20wslkT-pDHplJtsv7oZ4sS6fSVaIoQ"}}garbage', 'SXmBYCCsq0iNxm_IkWOJ1eVbMlbKz5Bx-_7T3iRhVvPz1Fq0ifP0F6OlgvVDZX4bxN3w-dEC7l3m307hGoyQBA')


def test_register_response():
    pk = smith.PublicKey.parse('AsYdXPAFS3ZzY9BPJKpsmaGuNV54Af-0AXhqHwkUQ5g')
    resp = smith.validate_register_response(pk, b'{"timestamp":"2018-02-28T16:14:24.084109Z","data":{"relay_id":"ba920727-03da-4596-925f-d8bb0f1f8ffd","token":"wcaRez_D3qIFK-NIqplNIEQWnRLZ01dcnCvJ0bCUCJjWfRQPCQSoD095Tp7X9HOUuT8nZKEFxDjIjd0zDA859A"}}', 'ueEi3iC61BhLsgk8gAADa_5vao9hG-q-DvaE-u9DnAUFRUlCBQExH45SrA_tgHyY7LiD-YLY5jVHmu3eXP64Bw', max_age=0xffffffff)
    assert resp['token'] == 'wcaRez_D3qIFK-NIqplNIEQWnRLZ01dcnCvJ0bCUCJjWfRQPCQSoD095Tp7X9HOUuT8nZKEFxDjIjd0zDA859A'
    assert resp['relay_id'] == uuid.UUID('ba920727-03da-4596-925f-d8bb0f1f8ffd')
