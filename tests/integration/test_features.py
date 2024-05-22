def test_features(relay_chain):
    relay = relay_chain()
    project_id = 42

    response = relay.get_remote_config(project_id)

    # TODO: Temporary hard-coded assertions. Once the stubs are removed this will fail.
    assert response["version"] == 0
    assert response["features"][0]["key"] == "hello"
    assert response["features"][0]["value"] == "world"
