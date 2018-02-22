import smith


def test_basic_key_functions():
    sk, pk = smith.generate_key_pair()
    signature = sk.sign(b'some secret data')
    assert pk.verify(b'some secret data', signature)
    assert not pk.verify(b'some other data', signature)
