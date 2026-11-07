"""
Belka · DAG de coleta diária (Airflow 3)

inicio >> [coletar_clima, coletar_populacao] >> conferir_chegada

Cada execução grava um arquivo por fonte em chegada/, com a data no nome.
Os arquivos saem em JSON Lines (um objeto por linha), formato que o
Auto Loader / COPY INTO do Databricks lê sem configuração extra.
"""
import json
import logging
from datetime import timedelta
from pathlib import Path

import pendulum
import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Variable, get_current_context, task

CHEGADA = Path("/opt/airflow/chegada")
FUSO = "America/Sao_Paulo"
OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
IBGE_POPULACAO = (
    "https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/-1/variaveis/9324"
)
log = logging.getLogger(__name__)


def dia_da_coleta() -> str:
    """Data da execução (ds). Em disparo manual sem data, usa ontem."""
    ds = get_current_context().get("ds")
    return ds or pendulum.now(FUSO).subtract(days=1).to_date_string()


def carregar_municipios() -> list[dict]:
    """Lista de municípios piloto, guardada na Variable belka_municipios."""
    return Variable.get("belka_municipios", deserialize_json=True)["municipios"]


def gravar_jsonl(destino: Path, linhas: list[dict]) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    with destino.open("w", encoding="utf-8") as f:
        for linha in linhas:
            f.write(json.dumps(linha, ensure_ascii=False) + "\n")
    log.info("Gravado %s (%d linhas)", destino, len(linhas))


def avisar_falha(context) -> None:
    """Notificação de falha: aparece no log e pode virar e-mail/Slack depois."""
    ti = context["task_instance"]
    log.error("BELKA | coleta falhou: tarefa=%s dia=%s", ti.task_id, context.get("ds"))


@task
def coletar_clima() -> str:
    dia = dia_da_coleta()
    municipios = carregar_municipios()
    r = requests.get(
        OPEN_METEO,
        params={
            "latitude": ",".join(str(m["lat"]) for m in municipios),
            "longitude": ",".join(str(m["lon"]) for m in municipios),
            "start_date": dia,
            "end_date": dia,
            "hourly": "temperature_2m,precipitation,relative_humidity_2m,wind_gusts_10m",
            "timezone": FUSO,
        },
        timeout=60,
    )
    r.raise_for_status()
    blocos = r.json()
    if isinstance(blocos, dict):  # 1 coordenada só devolve objeto, não lista
        blocos = [blocos]

    coletado_em = pendulum.now("UTC").to_iso8601_string()
    linhas = []
    for bloco, m in zip(blocos, municipios):  # a API mantém a ordem das coordenadas
        bloco.update(
            municipio_id=m["ibge"], municipio_nome=m["nome"], uf=m["uf"],
            dia_coleta=dia, fonte="open-meteo", coletado_em=coletado_em,
        )
        linhas.append(bloco)

    destino = CHEGADA / "clima" / f"clima_{dia}.json"
    gravar_jsonl(destino, linhas)
    return str(destino)


@task
def coletar_populacao() -> str:
    dia = dia_da_coleta()
    municipios = carregar_municipios()
    ids = ",".join(str(m["ibge"]) for m in municipios)
    r = requests.get(IBGE_POPULACAO, params={"localidades": f"N6[{ids}]"}, timeout=60)
    r.raise_for_status()

    resposta = r.json()[0]  # 1 variável pedida -> 1 objeto
    resposta.update(
        dia_coleta=dia, fonte="ibge-agregado-6579",
        coletado_em=pendulum.now("UTC").to_iso8601_string(),
    )
    destino = CHEGADA / "ibge" / f"ibge_populacao_{dia}.json"
    gravar_jsonl(destino, [resposta])
    return str(destino)


@task
def conferir_chegada(arquivos: list[str]) -> None:
    faltando = [a for a in arquivos if not Path(a).exists() or Path(a).stat().st_size == 0]
    if faltando:
        raise FileNotFoundError(f"Arquivos ausentes ou vazios: {faltando}")
    for a in arquivos:
        linhas = sum(1 for _ in Path(a).open(encoding="utf-8"))
        log.info("OK %s · %d linhas", a, linhas)


with DAG(
    dag_id="belka_coleta",
    description="Coleta diária de clima (Open-Meteo) e população (IBGE) para a Belka",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 25, tz=FUSO),
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "belka",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": avisar_falha,
    },
    tags=["belka", "gs-2026", "coleta"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    clima = coletar_clima()
    populacao = coletar_populacao()
    inicio >> [clima, populacao]
    conferir_chegada([clima, populacao])