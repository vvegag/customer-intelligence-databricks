"""
Testa que o modelo de Churn não usa, como feature, nenhuma coluna que também
entra na definição do próprio rótulo (churn_label).

Contexto: um audit encontrado achou vazamento de rótulo real neste projeto —
"03_gold/Feature Engineering Gold.py" define churn_label = 1 quando
(is_churned==1) OR (recency_days > 90 AND frequency > 0), e o modelo em
"04_models/Modelo Churn Prediction.py" incluía recency_days/frequency na
lista de features, ou seja, aprendia a própria fórmula do rótulo em vez de
comportamento preditivo real (métricas como AUC ficavam infladas por isso).
Esse teste é estático (regex/AST sobre o texto do notebook, sem Spark) pra
pegar regressão futura desse mesmo bug — se alguém reintroduzir
recency_days/frequency em feature_cols, ou expandir a fórmula do rótulo pra
incluir outra coluna que já é feature, o teste falha.

Escopo desta rodada: só o modelo de Churn. O Propensity Score tem um
vazamento análogo (mas por sobreposição de janela temporal, não por coluna
literal repetida) — não dá pra checar estaticamente sem antes redesenhar a
computação de RFM ponto-no-tempo; fica documentado como próximo passo no
plano, não coberto aqui.
"""
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

GOLD_FEATURE_ENGINEERING = REPO_ROOT / "03_gold" / "Feature Engineering Gold.py"
CHURN_MODEL_FILES = [
    REPO_ROOT / "04_models" / "Modelo Churn Prediction.py",
    REPO_ROOT / "04_models" / "AutoML Databricks Churn.py",
    REPO_ROOT / "05_scoring" / "Batch Scoring.py",
    REPO_ROOT / "04_models" / "Model_Explainability_SHAP.py",
]


def _extract_churn_label_source_columns() -> set[str]:
    """Extrai as colunas referenciadas via F.col(...) na definição de churn_label."""
    source = GOLD_FEATURE_ENGINEERING.read_text(encoding="utf-8")

    match = re.search(
        r'"churn_label",(.*?)\.otherwise\(0\)',
        source,
        flags=re.DOTALL,
    )
    assert match, (
        "Não encontrei a definição de churn_label em "
        f"{GOLD_FEATURE_ENGINEERING.name} — a fórmula mudou de formato? "
        "Ajuste o regex deste teste."
    )

    formula = match.group(1)
    columns = set(re.findall(r'F\.col\("([^"]+)"\)', formula))
    assert columns, "Regex não capturou nenhuma coluna na fórmula de churn_label"
    return columns


def _extract_feature_cols(path: Path) -> set[str]:
    """Extrai os literais de string da primeira lista `feature_cols = [...]` do arquivo."""
    source = path.read_text(encoding="utf-8")

    match = re.search(r"feature_cols\s*=\s*\[(.*?)\]", source, flags=re.DOTALL)
    assert match, f"Não encontrei feature_cols em {path.name}"

    block = match.group(1)
    return set(re.findall(r'"([a-zA-Z0-9_]+)"', block))


def test_churn_label_columns_documented():
    """Trava a fórmula esperada — se isso quebrar, a fórmula do rótulo mudou
    e as outras asserções deste arquivo precisam ser revisadas manualmente."""
    columns = _extract_churn_label_source_columns()
    assert columns == {"is_churned", "recency_days", "frequency"}, (
        f"churn_label agora usa {columns}, diferente do esperado — revise "
        "test_no_feature_overlaps_churn_label antes de seguir."
    )


def test_no_feature_overlaps_churn_label():
    label_columns = _extract_churn_label_source_columns()

    for path in CHURN_MODEL_FILES:
        assert path.exists(), f"Arquivo esperado não existe: {path}"
        feature_cols = _extract_feature_cols(path)
        leaked = feature_cols & label_columns
        assert not leaked, (
            f"{path.name}: feature_cols inclui {leaked}, que também definem "
            f"churn_label em {GOLD_FEATURE_ENGINEERING.name} — isso é "
            "vazamento de rótulo (o modelo aprende a fórmula do rótulo, não "
            "comportamento preditivo real)."
        )
