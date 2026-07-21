"""
Testa se as funções de configuração duplicadas em cada notebook (get_full_table_name,
create_or_replace_table) não divergiram entre si.

Contexto: os notebooks não usam um módulo compartilhado (ver ARQUITETURA/skill
databricks-mlops — %run com espaço no nome já quebrou o projeto antes), então cada
notebook copia essas funções manualmente. Esse teste existe pra pegar exatamente o
tipo de drift que causou bugs reais nesse projeto (cada notebook evoluindo sozinho,
divergindo silenciosamente).
"""
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# Todos os notebooks .py no fluxo principal + production/, exceto os que não usam
# o padrão de tabela (ex: notebooks só de integração externa sem Unity Catalog)
NOTEBOOK_GLOBS = ["0*/*.py", "production/*/*.py"]


def _all_notebooks():
    files = []
    for pattern in NOTEBOOK_GLOBS:
        files.extend(sorted(REPO_ROOT.glob(pattern)))
    return files


def _extract_function(source: str, func_name: str) -> str | None:
    """Extrai o corpo de uma função top-level pelo nome, sem depender de exec/import."""
    pattern = rf"^def {re.escape(func_name)}\(.*?(?=\n\S|\Z)"
    match = re.search(pattern, source, flags=re.MULTILINE | re.DOTALL)
    if not match:
        return None
    # Normaliza espaços em branco no fim de linha para não acusar diff cosmético
    return "\n".join(line.rstrip() for line in match.group(0).splitlines())


@pytest.mark.parametrize("func_name", ["get_full_table_name", "create_or_replace_table"])
def test_helper_function_has_no_drift(func_name):
    implementations = {}
    for notebook in _all_notebooks():
        source = notebook.read_text(encoding="utf-8")
        body = _extract_function(source, func_name)
        if body is not None:
            implementations[notebook.name] = body

    assert implementations, f"Nenhum notebook define {func_name} — o padrão mudou?"

    reference_file, reference_body = next(iter(implementations.items()))
    drifted = {
        name: body
        for name, body in implementations.items()
        if body != reference_body
    }
    assert not drifted, (
        f"{func_name} divergiu entre notebooks (referência: {reference_file}). "
        f"Arquivos com implementação diferente: {sorted(drifted)}"
    )


def test_get_full_table_name_uses_catalog_variable():
    """Garante que a função usa a variável CATALOG (não um nome de catálogo fixo),
    já que hardcode de catálogo já foi um bug real neste projeto."""
    for notebook in _all_notebooks():
        source = notebook.read_text(encoding="utf-8")
        body = _extract_function(source, "get_full_table_name")
        if body is None:
            continue
        assert "CATALOG" in body, (
            f"{notebook.name}: get_full_table_name não referencia a variável CATALOG"
        )
