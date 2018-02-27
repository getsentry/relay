import uuid
import smith
import pytest


def test_basic_key_functions():
    sk, pk = smith.generate_key_pair()
    signature = sk.sign(b'some secret data')
    assert pk.verify(b'some secret data', signature)
    assert not pk.verify(b'some other data', signature)


def test_challenge_response():
    resp = smith.create_register_challenge('JGk9dBcPbu8YVeoLorihRdjs-p0f1x-GTsjjEDwEAyQX6kVV438SjnGY7mmWc0N2TE8Ll2tBHeCIZAOVPbxIDg.eyJ0IjoiMjAxOC0wMi0yN1QxMDoxNjoxNC42ODEwOTJaIiwiZCI6eyJpIjoiNjBhYWIyYjktYzYwMS00ODNlLWEzOGMtZTBjNmU3YTUzMTg1IiwicCI6InFPVkhTYkhOZmdFT3ZCc1d0NF82T2ZmRnVsZzBaTGZSZ0UzNU1WRWlQdEEifX0', max_age=0xffffffff)
    assert str(resp['public_key']) == 'qOVHSbHNfgEOvBsWt4_6OffFulg0ZLfRgE35MVEiPtA'
    assert resp['relay_id'] == uuid.UUID('60aab2b9-c601-483e-a38c-e0c6e7a53185')
    assert len(resp['token']) > 40


def test_challenge_response_validation_errors():
    with pytest.raises(smith.UnpackErrorSignatureExpired):
        smith.create_register_challenge('JGk9dBcPbu8YVeoLorihRdjs-p0f1x-GTsjjEDwEAyQX6kVV438SjnGY7mmWc0N2TE8Ll2tBHeCIZAOVPbxIDg.eyJ0IjoiMjAxOC0wMi0yN1QxMDoxNjoxNC42ODEwOTJaIiwiZCI6eyJpIjoiNjBhYWIyYjktYzYwMS00ODNlLWEzOGMtZTBjNmU3YTUzMTg1IiwicCI6InFPVkhTYkhOZmdFT3ZCc1d0NF82T2ZmRnVsZzBaTGZSZ0UzNU1WRWlQdEEifX0', max_age=1)

    with pytest.raises(smith.UnpackErrorBadData):
        smith.create_register_challenge('JGk9dBcPbu8YVeoLorihRdjs-p0f1x-GTsjjEDwEAyQX6kVV438SjnGY7mmWc0N2TE8Ll2tBHeCIZAOVPbxIDg.eyJ0IjoiMjAxOC0wMi0yN1QxMDoxNjoxNC42ODEwOTJaIiwiZCI6eyJpIjoiNjBhYWIyYjktYzYwMS00ODNlLWEzOGMtZTBjNmU3YTUzMTg1IiwicCI6InFPVkhTYkhOZmdFT3ZCc1d0NF82T2ZmRnVsZzBaTGZSZ0UzNU1WRWlQdEEifX0addedcrapheres')


def test_register_response():
    pk = smith.PublicKey.parse('JsdkHWQ-H9-4kag2ybbyuahCEF2VAg3zf6kXpo-mj3A')
    resp = smith.validate_register_response(pk, 'NA5wJRV4a6-w7cFx14Krb3bBUowI0foNcV92s1yFa08_3KDPw4aDsM6ipfJkLjBozgRX0ws-7rvy_cA8771ADg.eyJ0IjoiMjAxOC0wMi0yN1QxMDo1ODo0NC4wMzIzMzZaIiwiZCI6eyJpIjoiZDcyNjIyOTItNzJkYy00MmMzLWI2NzctY2Q0YTFjZGMwMTJjIiwidCI6IkxfcmlZSi1QSEhrdy1QalVvYjRYV05fMnI1NXprR3pBQzlVR3dwcU5XRnhxNHFlWFJscUJmdGhQX3hRSjg0MnIxU3A0QUZVNUVvcDNPT25CMm1RNUpBIn19', max_age=0xffffffff)
    assert resp['token'] == 'L_riYJ-PHHkw-PjUob4XWN_2r55zkGzAC9UGwpqNWFxq4qeXRlqBfthP_xQJ842r1Sp4AFU5Eop3OOnB2mQ5JA'
    assert resp['relay_id'] == uuid.UUID('d7262292-72dc-42c3-b677-cd4a1cdc012c')
