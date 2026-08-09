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

O Propensity Score tinha um vazamento análogo, mas por sobreposição de janela
temporal (não por coluna literal repetida): o alvo `purchased_last_30d` e as
features de treino eram calculados na mesma data de referência, sem corte
temporal. Corrigido em "04_models/Modelo Propensity Score.py" recalculando as
features de treino ponto-no-tempo (só dado anterior a `cutoff_date`) via
`calcular_features_point_in_time`. O teste
`test_propensity_usa_features_point_in_time_no_treino`, abaixo, garante que
essa correção não seja revertida sem querer — não dá pra checar via
interseção feature∩label como no Churn (não é uma coluna repetida, é uma
questão de QUANDO a feature foi calculada), então o teste checa que a função
de corte temporal existe, é chamada com `cutoff_date` (não `max_date`), e
essa chamada acontece antes do `train_test_split` usado pra treinar.
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
PROPENSITY_MODEL_FILE = REPO_ROOT / "04_models" / "Modelo Propensity Score.py"


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


def test_propensity_usa_features_point_in_time_no_treino():
    """Garante que o Propensity continua treinando com features recalculadas
    ponto-no-tempo (data de corte), não com o estado atual de
    gold.customer_features — a diferença entre os dois é exatamente o que
    corrigiu o vazamento temporal (ver docstring do módulo)."""
    assert PROPENSITY_MODEL_FILE.exists(), f"Arquivo esperado não existe: {PROPENSITY_MODEL_FILE}"
    source = PROPENSITY_MODEL_FILE.read_text(encoding="utf-8")

    assert "def calcular_features_point_in_time(" in source, (
        "Não encontrei calcular_features_point_in_time em "
        f"{PROPENSITY_MODEL_FILE.name} — a correção de vazamento temporal foi "
        "removida ou renomeada?"
    )

    call_marker = "calcular_features_point_in_time(\n    df_transactions"
    call_idx = source.find(call_marker)
    assert call_idx != -1, (
        "Não encontrei a chamada de calcular_features_point_in_time() para "
        "montar as features de treino."
    )

    split_idx = source.find("train_test_split(X, y")
    assert split_idx != -1, "Não encontrei o train_test_split(X, y, ...) esperado."

    assert call_idx < split_idx, (
        "calcular_features_point_in_time() precisa ser chamada ANTES do "
        "train_test_split que gera X_train/X_test — do jeito que está, o "
        "treino pode estar usando features não recalculadas ponto-no-tempo."
    )

    # A chamada precisa usar cutoff_date (ponto-no-tempo), não max_date (estado atual)
    call_end = source.find(")", call_idx)
    trecho_chamada = source[call_idx:call_end]
    assert "cutoff_date" in trecho_chamada, (
        "A chamada de calcular_features_point_in_time() não referencia "
        "cutoff_date — sem isso, as features de treino voltam a usar a data "
        "de referência global (o mesmo vazamento de antes)."
    )
    assert "max_date" not in trecho_chamada.split("data_corte=")[-1], (
        "A chamada de calcular_features_point_in_time() parece estar usando "
        "max_date como data de corte, em vez de cutoff_date — isso reintroduz "
        "o vazamento temporal."
    )
