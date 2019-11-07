def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration_tests: mark test to be an integration test"
    )
    config.addinivalue_line(
        "markers", "unit_tests: mark test to be an unit test"
    )
