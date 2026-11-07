"""
Passo 1 · Belka Big Data
Testa as duas fontes do pipeline e salva amostras em amostras/.
Rodar na raiz do repositório:  python scripts/testar_fontes.py
Nenhuma das duas fontes precisa de chave.
"""
import json
from datetime import date, timedelta
from pathlib import Path

import requests

AMOSTRAS = Path("amostras")
AMOSTRAS.mkdir(exist_ok=True)

# Municípios piloto (código IBGE confirmado pela própria API abaixo)
MUNICIPIOS = {
    "São Paulo":       (-23.55, -46.63),
    "Guarulhos":       (-23.46, -46.53),
    "Franco da Rocha": (-23.32, -46.73),
    "Campinas":        (-22.91, -47.06),
    "São Sebastião":   (-23.76, -45.41),
}

# ---------- Fonte 1: Open-Meteo (clima, sem chave) ----------
ontem = (date.today() - timedelta(days=1)).isoformat()
r = requests.get(
    "https://api.open-meteo.com/v1/forecast",
    params={
        "latitude": ",".join(str(v[0]) for v in MUNICIPIOS.values()),
        "longitude": ",".join(str(v[1]) for v in MUNICIPIOS.values()),
        "start_date": ontem,
        "end_date": ontem,
        "hourly": "temperature_2m,precipitation,relative_humidity_2m,wind_gusts_10m",
        "daily": "temperature_2m_max,precipitation_sum,wind_gusts_10m_max",
        "timezone": "America/Sao_Paulo",
    },
    timeout=30,
)
r.raise_for_status()
clima = r.json()  # lista: um objeto por coordenada, na mesma ordem
(AMOSTRAS / f"clima_{ontem}.json").write_text(
    json.dumps(clima, ensure_ascii=False, indent=2), encoding="utf-8"
)
print(f"Open-Meteo OK: {len(clima)} localidades, "
      f"{len(clima[0]['hourly']['time'])} horas cada")

# ---------- Fonte 2: IBGE (municípios e população, sem chave) ----------
r = requests.get(
    "https://servicodados.ibge.gov.br/api/v1/localidades/estados/SP/municipios",
    timeout=30,
)
r.raise_for_status()
todos = {m["nome"]: m["id"] for m in r.json()}
codigos = {nome: todos[nome] for nome in MUNICIPIOS}
print("Códigos IBGE:", codigos)

ids = ",".join(str(c) for c in codigos.values())
r = requests.get(
    "https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/-1"
    f"/variaveis/9324?localidades=N6[{ids}]",
    timeout=30,
)
r.raise_for_status()
populacao = r.json()
(AMOSTRAS / "ibge_populacao.json").write_text(
    json.dumps(populacao, ensure_ascii=False, indent=2), encoding="utf-8"
)
for serie in populacao[0]["resultados"][0]["series"]:
    nome = serie["localidade"]["nome"]
    ano, valor = list(serie["serie"].items())[-1]
    print(f"  {nome}: {valor} habitantes ({ano})")

print("Amostras salvas em", AMOSTRAS.resolve())
