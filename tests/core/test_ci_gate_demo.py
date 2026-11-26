def test_ci_gate_rejects_a_failing_test():
    assert 1 + 1 == 3, "deliberate failure: CI must turn red (KAN-51 demonstration, never merged)"
