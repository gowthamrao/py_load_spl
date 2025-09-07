import pytest
from typer.testing import CliRunner
from testcontainers.postgres import PostgresContainer

from py_load_spl.cli import app
from py_load_spl.config import Settings, DatabaseSettings

runner = CliRunner()


def test_app_exists() -> None:
    """
    A very simple test to ensure the Typer app object can be imported.
    """
    assert app is not None


@pytest.mark.integration
def test_init_command(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test the 'init' command runs without errors against a test container.
    """
    with PostgresContainer("postgres:16-alpine") as postgres:
        # Create a settings object with the dynamic details from the container
        test_db_settings = DatabaseSettings(
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(5432),
            user=postgres.username,
            password=postgres.password,
            name=postgres.dbname,
            adapter="postgresql",
        )
        test_settings = Settings(db=test_db_settings)

        # Use monkeypatch to make the CLI use our test settings
        monkeypatch.setattr("py_load_spl.cli.get_settings", lambda: test_settings)

        # Run the command
        result = runner.invoke(app, ["init"])

        # Assert success
        assert result.exit_code == 0, f"CLI command failed with output:\n{result.stdout}"
        assert "Initializing database schema" in result.stdout
        assert "Schema initialization complete" in result.stdout
