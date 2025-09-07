from py_load_spl.cli import app
from typer.testing import CliRunner

runner = CliRunner()


def test_app_exists() -> None:
    """
    A very simple test to ensure the Typer app object can be imported.
    """
    assert app is not None


def test_init_command() -> None:
    """
    Test the 'init' command runs without errors.
    """
    result = runner.invoke(app, ["init"])
    assert result.exit_code == 0
    assert "Initializing database schema" in result.stdout
    assert "Schema initialization complete" in result.stdout
