"""
Verifica se todos os notebooks .py (formato "Databricks source") são Python
sintaticamente válido. Não executa nada (não tem spark/dbutils aqui), só garante
que não existe erro de sintaxe/indentação que só apareceria rodando no Databricks.
"""
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


def _all_py_notebooks():
    files = []
    for pattern in ["0*/*.py", "production/*/*.py"]:
        files.extend(sorted(REPO_ROOT.glob(pattern)))
    return files


@pytest.mark.parametrize("notebook", _all_py_notebooks(), ids=lambda p: p.name)
def test_notebook_compiles(notebook):
    source = notebook.read_text(encoding="utf-8")
    compile(source, str(notebook), "exec")
