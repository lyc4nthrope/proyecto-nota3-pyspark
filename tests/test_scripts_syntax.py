"""Guards scripts/setup_winutils.ps1 against PowerShell parse errors.

The setup script is the only Windows bootstrap step in the README, so it must
compile on Windows PowerShell 5.1 before anyone follows the instructions.
A '$name:' reference inside a double-quoted string is a fatal parse error that
stops the whole script before line 1 runs, and nothing else in the suite
executes the script.
"""

import pathlib
import shutil
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "setup_winutils.ps1"

HAS_POWERSHELL = shutil.which("powershell") is not None


def test_setup_winutils_parses_without_errors():
    """PowerShell must compile the script without any parse error."""
    if not HAS_POWERSHELL:
        pytest.skip("PowerShell is not available on this machine")
    probe = (
        "$e=$null;$t=$null;"
        f"[System.Management.Automation.Language.Parser]::ParseFile('{SCRIPT}',[ref]$t,[ref]$e)|Out-Null;"
        "if($e.Count -gt 0){ $e | ForEach-Object { $_.ToString() }; exit 1 }"
    )
    result = subprocess.run(
        ["powershell", "-NoProfile", "-Command", probe],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_hash_constants_match_documented_prefixes():
    """The pinned hashes are exactly the ones docs/BITACORA.md quotes."""
    text = SCRIPT.read_text(encoding="utf-8-sig")
    assert (
        "496A591EB1E67DF2A620F710D529BA6DDFE1C19149E6647CC4E320BB0EFD8553" in text
    )
    assert "D7AB36A68518748CEF142BE2DA5069B4C763C2CD764C1D2E6AC48C7200405BE3" in text
