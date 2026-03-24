# Guia para Claude: Extração de Estrutura de BI via Power BI REST API + DAX

> **Audiência**: Claude Code / Claude em conversas com o time BGP.
> Este documento ensina como extrair programaticamente filtros, medidas, tabelas e fórmulas DAX de qualquer relatório Power BI — sem abrir o Power BI Desktop.
> Use este guia sempre que o usuário pedir para entender, documentar ou replicar a lógica de um BI.

---

## Credenciais BGP (Azure AD)

As credenciais ficam no `.env` do projeto. Se não estiverem disponíveis, pergunte ao usuário.
Os scopes necessários são:

- `Dataset.Read.All`, `Dataset.ReadWrite.All`
- `Report.Read.All`, `Report.ReadWrite.All`
- `Workspace.Read.All`
- `Content.Create` (para migração de datasets)

---

## Passo 1: Autenticar

Use o flow `authorization_code` com servidor HTTP local (interativo) ou `client_credentials` (service principal).

```python
import requests

TENANT_ID = "do .env"
CLIENT_ID = "do .env"
CLIENT_SECRET = "do .env"
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
BASE = "https://api.powerbi.com/v1.0/myorg"

# Service principal (automação)
resp = requests.post(TOKEN_URL, data={
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "grant_type": "client_credentials",
    "scope": "https://analysis.windows.net/powerbi/api/.default",
})
token = resp.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}
```

Para flow interativo (authorization_code), subir servidor local na porta 8000, abrir browser, capturar o code no redirect e trocar por token. Veja `bi_relatorio.R` ou `pbi_extract_esa.py` como referência.

---

## Passo 2: Localizar o Dataset

```python
# Listar workspaces
workspaces = requests.get(f"{BASE}/groups", headers=headers).json()["value"]

# Datasets de um workspace específico
datasets = requests.get(f"{BASE}/groups/{ws_id}/datasets", headers=headers).json()["value"]

# Datasets do "My workspace" (endpoint diferente — sem /groups/)
my_datasets = requests.get(f"{BASE}/datasets", headers=headers).json()["value"]
```

Cada dataset tem: `id`, `name`, `createdDate`, `configuredBy`, `isRefreshable`, `targetStorageMode`.

---

## Passo 3: Descobrir Fontes de Dados

Mostra de onde o BI puxa os dados (Excel, SQL Server, API, etc).

```python
sources = requests.get(f"{BASE}/datasets/{ds_id}/datasources", headers=headers).json()["value"]

for src in sources:
    tipo = src["datasourceType"]           # "File", "Sql", "Web", etc
    conn = src.get("connectionDetails", {})
    server = conn.get("server")            # host SQL
    database = conn.get("database")        # nome do banco
    path = conn.get("path")               # caminho de arquivo (Excel/CSV)
    url = conn.get("url")                 # URL de API/web
```

> **Dica**: se a fonte for arquivo Excel local, pode ser mais rápido ler o Excel direto do que passar pelo Power BI.

---

## Passo 4: Exportar o .pbix

O .pbix é a fonte mais rica de metadados. Exportar via API:

```python
pbix = requests.get(f"{BASE}/reports/{report_id}/Export", headers=headers).content

with open("export.pbix", "wb") as f:
    f.write(pbix)
```

> Para reports no "My workspace", use `f"{BASE}/reports/{report_id}/Export"` (sem /groups/).

---

## Passo 5: Extrair Fórmulas DAX das Medidas (pbixray)

O DataModel dentro do .pbix usa compressão XPress9. A biblioteca `pbixray` descompacta e extrai tudo.

```bash
pip install pbixray
```

```python
from pbixray import PBIXRay

model = PBIXRay("export.pbix")

# MEDIDAS com fórmulas DAX completas
measures = model.dax_measures
# DataFrame com colunas: TableName, Name, Expression
# Exemplo:
#   BASE CONTA AZUL | Valor líquido | SUM(...[receita]) - SUM(...[despesa]) + SUM(...[emprestimos])
#   BASE CONTA AZUL | EBITDA        | CALCULATE([Entrada Líquida]) - CALCULATE(SUM(...), ...)

# TABELAS
tables = model.tables

# SCHEMA (colunas de cada tabela)
schema = model.schema

# COLUNAS CALCULADAS DAX
calc_columns = model.dax_columns

# RELACIONAMENTOS entre tabelas
relationships = model.relationships

# POWER QUERY (M) — código de transformação de cada tabela
pq = model.power_query

# ESTATÍSTICAS (tamanho, cardinalidade)
stats = model.statistics
```

> **Sempre use pbixray para obter fórmulas.** A API REST não retorna fórmulas DAX de medidas.

---

## Passo 6: Extrair Filtros, Slicers e Layout das Páginas

O layout do report fica em `Report/Layout` dentro do .pbix — é um JSON codificado em UTF-16LE.

```python
import zipfile, json

z = zipfile.ZipFile("export.pbix")
layout = json.loads(z.read("Report/Layout").decode("utf-16-le"))
```

### 6.1 Listar páginas

```python
for sec in layout["sections"]:
    print(sec["displayName"])
```

### 6.2 Encontrar todas as tabelas usadas no report

```python
entities = set()

def find_entities(obj):
    if isinstance(obj, str):
        try: find_entities(json.loads(obj))
        except: pass
    elif isinstance(obj, dict):
        if "Entity" in obj and isinstance(obj["Entity"], str):
            entities.add(obj["Entity"])
        for v in obj.values(): find_entities(v)
    elif isinstance(obj, list):
        for item in obj: find_entities(item)

find_entities(layout)
# entities agora contém todos os nomes de tabela referenciados
```

### 6.3 Extrair slicers (filtros interativos)

```python
for sec in layout["sections"]:
    page = sec.get("displayName", "?")
    for vc in sec.get("visualContainers", []):
        try: cfg = json.loads(vc.get("config", "{}"))
        except: continue
        sv = cfg.get("singleVisual", {})
        if sv.get("visualType") != "slicer": continue

        for sel in sv.get("prototypeQuery", {}).get("Select", []):
            for key in ["Column", "Measure"]:
                item = sel.get(key, {})
                col_expr = item.get("Expression", {}).get("Column", {})
                if col_expr:
                    entity = col_expr.get("Expression", {}).get("SourceRef", {}).get("Entity", "")
                    prop = col_expr.get("Property", "")
                    print(f"[{page}] Slicer: {entity}.{prop}")
```

### 6.4 Extrair filtros fixos de página

```python
for sec in layout["sections"]:
    page = sec.get("displayName", "?")
    for f in json.loads(sec.get("filters", "[]")):
        expr = f.get("expression", {}).get("Column", {})
        entity = expr.get("Expression", {}).get("SourceRef", {}).get("Entity", "")
        col = expr.get("Property", "")
        ftype = f.get("type", "")
        print(f"[{page}] Filtro fixo ({ftype}): {entity}.{col}")
        # f["filter"]["Where"] contém a condição (ex: receita > 0)
```

### 6.5 Extrair medidas e agregações usadas em cada visual

```python
for sec in layout["sections"]:
    page = sec.get("displayName", "?")
    for vc in sec.get("visualContainers", []):
        try: cfg = json.loads(vc.get("config", "{}"))
        except: continue
        sv = cfg.get("singleVisual", {})
        vis_type = sv.get("visualType", "")

        for sel in sv.get("prototypeQuery", {}).get("Select", []):
            m = sel.get("Measure", {})
            if m:
                print(f"[{page}] [{vis_type}] Medida: {m.get('Property')}")
            a = sel.get("Aggregation", {})
            if a:
                col = a.get("Expression", {}).get("Column", {})
                if col:
                    print(f"[{page}] [{vis_type}] Agg({a.get('Function')}): {col.get('Property')}")
```

---

## Passo 7: Executar DAX Queries (extrair dados reais)

### REGRA: executeQueries NÃO funciona no "My workspace"

Retorna erro 400 (`DatasetExecuteQueriesError`). Funciona em qualquer workspace regular, mesmo Pro (sem Premium).

### Migrar dataset do My workspace para workspace regular

```python
# Exportar .pbix (já feito no passo 4)
# Importar no workspace destino:
resp = requests.post(
    f"{BASE}/groups/{ws_id}/imports?datasetDisplayName=MeuBI&nameConflict=CreateOrOverwrite",
    headers={"Authorization": f"Bearer {token}"},
    files={"file": ("export.pbix", open("export.pbix", "rb"), "application/octet-stream")},
)
import_id = resp.json()["id"]

# Aguardar (polling)
import time
while True:
    st = requests.get(f"{BASE}/groups/{ws_id}/imports/{import_id}", headers=headers).json()
    if st["importState"] == "Succeeded":
        new_ds_id = st["datasets"][0]["id"]
        break
    time.sleep(3)
```

### Executar DAX

```python
def dax_query(ws_id, ds_id, query):
    r = requests.post(
        f"{BASE}/groups/{ws_id}/datasets/{ds_id}/executeQueries",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"queries": [{"query": query}], "serializerSettings": {"includeNulls": True}},
    )
    r.raise_for_status()
    return r.json()["results"][0]["tables"][0]["rows"]

# Teste de conexão
dax_query(ws_id, ds_id, 'EVALUATE ROW("ok", 1)')

# Extrair dados de uma tabela
rows = dax_query(ws_id, ds_id, "EVALUATE TOPN(5000, 'NomeDaTabela')")
df = pd.DataFrame(rows)
df.columns = [c.split("[")[-1].rstrip("]") if "[" in c else c for c in df.columns]
```

### DMVs (metadados via query)

A maioria das DMVs **não funciona** via executeQueries (erro 400):

| DMV | Funciona? | Retorna fórmula? |
|-----|-----------|------------------|
| `$SYSTEM.MDSCHEMA_MEASURES` | Sim (parcial, 1 row) | Não |
| `$SYSTEM.TMSCHEMA_MEASURES` | Não | - |
| `$SYSTEM.TMSCHEMA_TABLES` | Não | - |
| `INFO.TABLES()` | Não | - |
| `INFO.COLUMNS()` | Não | - |
| `INFO.MEASURES()` | Não | - |

**Conclusão: use pbixray para metadados, DAX queries só para extrair dados.**

---

## Passo 8: Limpeza de Datasets Antigos

Para identificar datasets abandonados no My workspace:

```python
from datetime import datetime, timezone, timedelta
cutoff = datetime.now(timezone.utc) - timedelta(days=18 * 30)  # 18 meses

for ds in my_datasets:
    # Checar último refresh
    ref = requests.get(f"{BASE}/datasets/{ds['id']}/refreshes?$top=1", headers=headers)
    refreshes = ref.json().get("value", []) if ref.ok else []

    if refreshes:
        last = datetime.fromisoformat(refreshes[0]["endTime"].replace("Z", "+00:00"))
        if last < cutoff:
            print(f"REFRESH ANTIGO: {ds['name']} ({last.date()})")
    else:
        # Sem refresh = atualizado na mão. Usar createdDate como proxy.
        created = datetime.fromisoformat(ds["createdDate"].replace("Z", "+00:00"))
        if created < cutoff:
            print(f"SEM REFRESH + ANTIGO: {ds['name']} ({created.date()})")
```

> **Regra**: datasets atualizados manualmente (upload de .pbix) NÃO geram histórico de refresh. O campo `createdDate` é atualizado no re-upload, então serve como proxy de "última atualização".

Para deletar: primeiro remover os reports associados, depois o dataset.

```python
# Reports do dataset
for r in all_reports:
    if r.get("datasetId") == ds_id:
        requests.delete(f"{BASE}/reports/{r['id']}", headers=headers)

# Dataset
requests.delete(f"{BASE}/datasets/{ds_id}", headers=headers)
```

---

## Referência Rápida

| Tarefa | Como | Onde |
|--------|------|------|
| Listar workspaces | `GET /groups` | API |
| Listar datasets | `GET /groups/{id}/datasets` ou `GET /datasets` | API |
| Ver fontes de dados | `GET /datasets/{id}/datasources` | API |
| Exportar .pbix | `GET /reports/{id}/Export` | API |
| Fórmulas DAX | `PBIXRay("file.pbix").dax_measures` | Offline (.pbix) |
| Schema/tabelas/colunas | `PBIXRay("file.pbix").schema` | Offline (.pbix) |
| Relacionamentos | `PBIXRay("file.pbix").relationships` | Offline (.pbix) |
| Filtros e slicers | Parse de `Report/Layout` no .pbix | Offline (.pbix) |
| Medidas por visual | Parse de `Report/Layout` no .pbix | Offline (.pbix) |
| Dados reais (tabelas) | DAX `EVALUATE TOPN(N, 'Tabela')` | API (workspace regular) |
| Migrar p/ workspace | Export .pbix + Import via API | API |
| Refresh history | `GET /datasets/{id}/refreshes` | API |
| Deletar dataset | `DELETE /datasets/{id}` | API |

---

## Fluxo Completo Recomendado

Quando o usuário pedir para entender a estrutura de um BI:

1. **Autenticar** (OAuth2)
2. **Localizar** o dataset (listar workspaces/datasets)
3. **Exportar .pbix** via API
4. **pbixray** no .pbix: tabelas, schema, medidas com fórmulas, relacionamentos
5. **Report/Layout** no .pbix: páginas, filtros, slicers, visuais, medidas por visual
6. Se precisar dos **dados reais**: importar em workspace regular e rodar DAX queries
7. Consolidar tudo e apresentar ao usuário
