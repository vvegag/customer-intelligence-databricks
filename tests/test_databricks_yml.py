"""
Valida a estrutura do databricks.yml (Asset Bundle) sem precisar de credenciais
Databricks — pega localmente os dois bugs reais que esse arquivo já teve: um job
definido fora de `resources.jobs` por erro de indentação, e `notebook_path`
apontando para arquivos que foram renomeados/movidos e não existem mais.
"""
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
BUNDLE_PATH = REPO_ROOT / "databricks.yml"


def _load_bundle() -> dict:
    with open(BUNDLE_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def test_bundle_file_is_valid_yaml():
    bundle = _load_bundle()
    assert bundle["bundle"]["name"]


def test_resources_jobs_exist():
    bundle = _load_bundle()
    jobs = bundle.get("resources", {}).get("jobs")
    assert jobs, "resources.jobs vazio ou ausente"
    assert isinstance(jobs, dict)


def test_every_job_key_is_nested_under_resources_jobs():
    """Regressão do bug real: ml_training_pipeline estava definido como irmão de
    `jobs:` em vez de dentro dele — o bundle validator não acusa isso como erro de
    sintaxe YAML (é YAML válido), só como 'campo desconhecido' na hora do deploy."""
    bundle = _load_bundle()
    resources = bundle.get("resources", {})
    # Únicas chaves esperadas diretamente sob `resources`: tipos de recurso do DAB
    known_resource_types = {"jobs", "pipelines", "experiments", "models", "volumes", "schemas"}
    unexpected = set(resources.keys()) - known_resource_types
    assert not unexpected, (
        f"Chave(s) inesperada(s) direto sob 'resources': {unexpected} — "
        f"provavelmente um job com indentação errada (deveria estar dentro de 'jobs:')"
    )


def test_all_notebook_paths_exist_on_disk():
    bundle = _load_bundle()
    jobs = bundle.get("resources", {}).get("jobs", {})
    missing = []
    for job_name, job_def in jobs.items():
        for task in job_def.get("tasks", []):
            notebook_task = task.get("notebook_task")
            if not notebook_task:
                continue
            rel_path = notebook_task["notebook_path"].lstrip("./")
            full_path = REPO_ROOT / rel_path
            if not full_path.exists():
                missing.append(f"{job_name}/{task['task_key']}: {rel_path}")
    assert not missing, "notebook_path apontando para arquivo inexistente:\n" + "\n".join(missing)


def test_all_declared_variables_are_used_or_have_default():
    """Regressão do bug real: num_workers era usado nos targets sem estar
    declarado em `variables:` no topo do arquivo."""
    bundle = _load_bundle()
    declared_vars = set(bundle.get("variables", {}).keys())
    used_vars = set()
    for target in bundle.get("targets", {}).values():
        used_vars.update(target.get("variables", {}).keys())
    undeclared = used_vars - declared_vars
    assert not undeclared, (
        f"Variável(is) usada(s) em targets mas não declarada(s) em 'variables': {undeclared}"
    )


def test_production_targets_have_root_path():
    """Bundles em mode: production exigem workspace.root_path explícito."""
    bundle = _load_bundle()
    for target_name, target in bundle.get("targets", {}).items():
        if target.get("mode") == "production":
            assert target.get("workspace", {}).get("root_path"), (
                f"Target '{target_name}' está em mode: production mas não define workspace.root_path"
            )
