import uuid
import smith


def test_basic_key_functions():
    sk, pk = smith.generate_key_pair()
    signature = sk.sign(b'some secret data')
    assert pk.verify(b'some secret data', signature)
    assert not pk.verify(b'some other data', signature)


def test_challenge_response():
    resp = smith.create_register_challenge('scytiiMDtFO8GfpMHYvOs0Ue7mvb_jZmN7dAPb70KS_nu1FH3UuiZ0LsbLDu35_r5ohSqdD0llvsSZLTRmflDQ.eyJ0IjoiMjAxOC0wMi0yMlQyMzoxMDoyNy40MTQ0NjVaIiwiZCI6eyJhZ2VudF9pZCI6ImExNDEyNjIwLWIxNDgtNDJiOS1iNmFmLTZlM2YzYmFlMDhkYSIsInB1YmxpY19rZXkiOiJyQm5HbEU0czc5c1FJaGtraDE0aGRBSExTNlhsc1A0LUQxSmxFRXZ5aG1zIn19',  max_age=0xffffffff)
    assert str(resp['public_key']) == 'rBnGlE4s79sQIhkkh14hdAHLS6XlsP4-D1JlEEvyhms'
    assert resp['agent_id'] == uuid.UUID('a1412620-b148-42b9-b6af-6e3f3bae08da')
    assert len(resp['token']) > 40


def test_register_response():
    pk = smith.PublicKey.parse('cv_JmtmGfMlpX82UCHwB8txCNm4kOQX8f8dMYEQv7Ts')
    resp = smith.validate_register_response(pk, 'zgm-GMnQgFVaFG9qyGBsrZvHFnf-n1A4xg5n_NrRm0GKZthQFOcjDUsMvcytMY6ivmUaVjTL-Y1zmwEl5jGCBQ.eyJ0IjoiMjAxOC0wMi0yMlQyMzoyODowMC42NjcxOTlaIiwiZCI6eyJhZ2VudF9pZCI6IjdkNGY2M2Q2LTVmZGUtNGEyMS1iMmQ2LTY4OGViNGZhNTM1MCIsInRva2VuIjoiNzU1amVyWlh6bTZ5ZFAwRWg0ZmJRcmRJbTB4ZXVWNkR3NlZCbGNJUTF4QmZZYUlBRWsxNjBSNW1WdHVWYXdxWF82OU5RWnJKcHFKMUVRdktZc0VzWVEifX0', max_age=0xffffffff)
    assert resp['token'] == '755jerZXzm6ydP0Eh4fbQrdIm0xeuV6Dw6VBlcIQ1xBfYaIAEk160R5mVtuVawqX_69NQZrJpqJ1EQvKYsEsYQ'
    assert resp['agent_id'] == uuid.UUID('7d4f63d6-5fde-4a21-b2d6-688eb4fa5350')
