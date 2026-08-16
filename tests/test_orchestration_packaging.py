from __future__ import annotations

import ast
from pathlib import Path


def test_pyinstaller_sidecar_collects_orchestration_sql_migrations() -> None:
    repository = Path(__file__).resolve().parents[1]
    spec_path = repository / "packaging" / "openworker-server.spec"
    tree = ast.parse(spec_path.read_text(encoding="utf-8"), filename=str(spec_path))

    hook_imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == "PyInstaller.utils.hooks"
        for alias in node.names
    }
    assert "collect_data_files" in hook_imports

    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "collect_data_files"
    ]
    assert calls, "the sidecar spec must explicitly collect package data"

    includes: list[str] = []
    packages: list[str] = []
    for call in calls:
        if call.args and isinstance(call.args[0], ast.Constant):
            packages.append(str(call.args[0].value))
        for keyword in call.keywords:
            if keyword.arg != "includes" or not isinstance(keyword.value, (ast.List, ast.Tuple)):
                continue
            includes.extend(
                str(item.value)
                for item in keyword.value.elts
                if isinstance(item, ast.Constant)
            )

    assert "coworker" in packages
    assert "orchestration/migrations/*.sql" in includes
    assert list((repository / "coworker" / "orchestration" / "migrations").glob("*.sql"))
