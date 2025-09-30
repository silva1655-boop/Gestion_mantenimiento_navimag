# storage_hf.py
# Persistencia en Hugging Face Dataset Repo con lectura/escritura segura
# API pública: load_state(), save_state(state, message), export_csv_parquet(state, out_dir)

import os
import io
import json
import time
import pathlib
from typing import Dict, Any, Optional

from huggingface_hub import (
    HfApi,
    HfFolder,
    hf_hub_download,
    create_repo,
    Repository,
)

# =========================
# Configuración por entorno
# =========================
HF_DATASET_REPO = os.getenv("HF_DATASET_REPO", "tu_usuario/fleet-db")   # ej: "usuario/fleet-db"
DB_FILE         = os.getenv("HF_DB_FILE", "state/maintenance_data.json")
LOCAL_MIRROR    = os.getenv("HF_LOCAL_MIRROR", "/home/user/app_data")   # carpeta local en el Space

# =========================
# Utilidades internas
# =========================
def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def _atomic_write(path: pathlib.Path, data: str, encoding: str = "utf-8") -> None:
    """Escritura atómica: escribe a .tmp y luego renombra."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding=encoding) as f:
        f.write(data)
    tmp.replace(path)

def _ensure_repo() -> Repository:
    """
    Clona o abre el repo de dataset en LOCAL_MIRROR.
    Hace git pull rebase al inicio. Devuelve handler Repository.
    """
    token = HfFolder.get_token()
    if not token:
        raise RuntimeError("HF_TOKEN no configurado en Secrets.")

    # Crea repo si no existe
    create_repo(repo_id=HF_DATASET_REPO, repo_type="dataset", exist_ok=True, token=token)

    # Clona/abre
    pathlib.Path(LOCAL_MIRROR).mkdir(parents=True, exist_ok=True)
    repo_url = f"https://huggingface.co/datasets/{HF_DATASET_REPO}"
    repo = Repository(
        local_dir=LOCAL_MIRROR,
        clone_from=repo_url,
        repo_type="dataset",
        use_auth_token=token,
        skip_lfs_files=True,  # más rápido para JSON/CSV
    )

    # Config de identidad (algunos entornos lo requieren para commit)
    try:
        repo.git_config_username_email(username="hf-space-bot", email="no-reply@users.noreply.huggingface.co")
    except Exception:
        pass

    # Pull inicial (mejor esfuerzo)
    try:
        repo.git_pull(rebase=True)
    except Exception:
        pass

    return repo

def _safe_push(repo: Repository, rel_path: str, message: str, max_retries: int = 3) -> None:
    """
    Commit + push con reintentos. En caso de rechazo remoto, hace pull --rebase y reintenta.
    """
    for i in range(max_retries):
        try:
            repo.git_add(rel_path)
            repo.git_commit(f"[{_now_ts()}] {message}")
        except Exception as e:
            # Si no hay cambios (nothing to commit), intentar push directo
            # o seguir al siguiente paso
            pass
        try:
            repo.git_push()
            return
        except Exception:
            # Conflicto o rechazo; intentar rebasear
            try:
                repo.git_pull(rebase=True)
            except Exception:
                time.sleep(0.8)
            time.sleep(0.8)
    # Último intento "forzado"
    try:
        repo.git_push()
    except Exception as e:
        raise RuntimeError(f"No se pudo hacer push tras reintentos: {e}")

# =========================
# API pública
# =========================
def load_state() -> Dict[str, Any]:
    """
    Lee el JSON del repo (si no existe, devuelve estado vacío).
    Siempre intenta repo.git_pull antes de leer.
    """
    repo = _ensure_repo()
    fpath = pathlib.Path(LOCAL_MIRROR) / DB_FILE

    # Asegura árbol más reciente
    try:
        repo.git_pull(rebase=True)
    except Exception:
        pass

    # 1) Intenta leer del espejo local
    if fpath.exists():
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            # Si el archivo local está corrupto, intenta bajar del hub
            pass

    # 2) Si no está en local o falló, intenta bajar directo del hub
    try:
        cached = hf_hub_download(
            repo_id=HF_DATASET_REPO,
            repo_type="dataset",
            filename=DB_FILE,
            revision="main",
        )
        with open(cached, "r", encoding="utf-8") as f:
            state = json.load(f)
        # Sincroniza al espejo local
        _atomic_write(fpath, json.dumps(state, ensure_ascii=False, indent=2))
        return state
    except Exception:
        # Primera ejecución (o aún no hay JSON)
        return {}

def save_state(state: Dict[str, Any], message: str = "update") -> None:
    """
    Escribe el JSON en LOCAL_MIRROR/DB_FILE y hace commit/push al dataset repo.
    Con reintentos y pull --rebase si hay conflicto.
    """
    repo = _ensure_repo()
    fpath = pathlib.Path(LOCAL_MIRROR) / DB_FILE

    # Escribe atómicamente
    payload = json.dumps(state, ensure_ascii=False, indent=2)
    _atomic_write(fpath, payload)

    # Path relativo para git add
    rel = str(fpath.relative_to(LOCAL_MIRROR))

    # Push con reintentos
    _safe_push(repo, rel, message)

def export_csv_parquet(state: Dict[str, Any], out_dir: str) -> Dict[str, str]:
    """
    Crea copias CSV/Parquet descargables:
      - equipos
      - ots (pendientes/completadas)
      - fallas
      - solicitudes
      - inventario
      - usuarios (solo usernames y roles; nunca contraseñas)

    Retorna dict {nombre_logico: ruta_csv}.
    """
    import pandas as pd

    p = pathlib.Path(out_dir); p.mkdir(parents=True, exist_ok=True)
    files: Dict[str, str] = {}

    # -------- equipos --------
    eq = state.get("fleet", {})
    df_eq = pd.DataFrame([
        {
            "id": k,
            "description": v.get("description",""),
            "horometro": v.get("horometro",0.0),
            "odometro": v.get("odometro",0.0),
            "status": v.get("status",""),
            # si más adelante agregas ubicacion al estado, aquí se mapea:
            "ubicacion": v.get("ubicacion",""),
        } for k, v in eq.items()
    ])

    # -------- ots --------
    ots = state.get("pending_orders", [])
    df_ots = pd.DataFrame([
        {
            "id": o.get("id",""),
            "equipment_id": o.get("equipment_id",""),
            "component_name": o.get("component_name",""),
            "classification": o.get("classification",""),
            "due_date": o.get("due_date",""),
            "reason": o.get("reason",""),
            "status": o.get("status",""),
            "created_at": o.get("created_at",""),
            "start_time": o.get("start_time"),
            "completed_at": o.get("completed_at"),
            "materials_used": ", ".join(o.get("materials_used", [])) if o.get("materials_used") else "",
        } for o in ots
    ])

    # -------- fallas --------
    fls = state.get("failures", [])
    df_fallas = pd.DataFrame([
        {
            "timestamp": f.get("timestamp",""),
            "equipment_id": f.get("equipment_id",""),
            "component_name": f.get("component_name",""),
            "description": f.get("description",""),
            "repair_time_hours": f.get("repair_time_hours",0.0),
        } for f in fls
    ])

    # -------- solicitudes --------
    reqs = state.get("work_requests", [])
    df_reqs = pd.DataFrame([
        {
            "id": r.get("id",""),
            "equipment_id": r.get("equipment_id",""),
            "component_name": r.get("component_name",""),
            "classification": r.get("classification",""),
            "comments": r.get("comments",""),
            "horometro": r.get("horometro",0.0),
            "date": r.get("date",""),
            "status": r.get("status",""),
            "created_at": r.get("created_at",""),
        } for r in reqs
    ])

    # -------- inventario --------
    inv = state.get("inventory", {})
    df_inv = pd.DataFrame([
        {
            "part_name": k,
            "stock": v.get("stock",0),
            "min_stock": v.get("min_stock",0),
            "fits_components": ", ".join(v.get("fits_components", [])) if v.get("fits_components") else "",
        } for k, v in inv.items()
    ])

    # -------- usuarios (safe) --------
    users = state.get("users", {})
    df_users = pd.DataFrame([
        {"username": u, "role": (info or {}).get("role","")}
        for u, info in users.items()
    ])

    # Helper para escribir CSV y Parquet
    def _dump(df: pd.DataFrame, name: str):
        csv_path = p / f"{name}.csv"
        pq_path  = p / f"{name}.parquet"
        # Evitar fallos si df está vacío: crear CSV con encabezados
        if df is None or df.empty:
            df = pd.DataFrame()
        df.to_csv(csv_path, index=False)
        try:
            # Requiere pyarrow en requirements.txt
            df.to_parquet(pq_path, index=False)
        except Exception:
            pass
        files[name] = str(csv_path)

    _dump(df_eq,      "equipos")
    _dump(df_ots,     "ots")
    _dump(df_fallas,  "fallas")
    _dump(df_reqs,    "solicitudes")
    _dump(df_inv,     "inventario")
    _dump(df_users,   "usuarios")

    return files
