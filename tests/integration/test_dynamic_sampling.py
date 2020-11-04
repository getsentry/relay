def test_it_removes_transactions():
    """
    Tests that when sampling is set to 0% for the trace context project the transactions are removed
    """
    pass


def test_it_keeps_transactions(mini_sentry, relay):
    """
    Tests that when sampling is set to 100% for the trace context project the transactions are kept
    """
    relay = relay(mini_sentry)
    relay.basic_project_config()
