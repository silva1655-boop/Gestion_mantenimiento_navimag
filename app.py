# app.py
"""
Maintenance Management Application with Role-based Interface
------------------------------------------------------------

Esta versión está integrada con persistencia en Hugging Face Dataset Repo,
usando storage_hf.load_state()/save_state() para que los datos NO se pierdan
al cerrar la app y se puedan descargar como CSV/Parquet.

Requisitos:
- Subir a tu Space (Streamlit): app.py, maintenance_program.py, storage_hf.py,
  requirements.txt (con streamlit, pandas, huggingface_hub, datasets).
- Configurar en el Space:
  Secrets: HF_TOKEN (write)
  Variables: HF_DATASET_REPO, HF_DB_FILE, HF_LOCAL_MIRROR

Ejecución local (opcional):
  streamlit run app.py
"""

import datetime
import calendar
import os
import uuid
import json
from typing import Dict, List, Tuple

import streamlit as st
import pandas as pd

from maintenance_program import (
    Component,
    Equipment,
    Scheduler,
    Inventory,
    FailureLog,
    WorkOrder,
)

# NUEVO: persistencia en Hugging Face dataset repo
from storage_hf import (
    load_state as hf_load_state,
    save_state as hf_save_state,
    export_csv_parquet,
)

# ---------------------------------------------------------------------------
# Utilidades

def play_alert_sound() -> None:
    """Reproduce un beep corto embebido en base64 (sin archivos externos)."""
    BEEP_BASE64 = (
        "UklGRgxFAABXQVZFZm10IBAAAAABAAEARKwAAIhYAQACABAAZGF0YehEAAAAAAAAPwAA"
        "PT8AAP7/AAH+fwAC/n4AB/9+ABX/fgAZ/34AIf5+ADI/fgArPn4AHj1+AB08fgAZPH4A"
        "DDp+ABs3fgAkMH4AEzV+AAQxfv8BPn3/AT99/wH/fv8C/n7/A/9+/wT/ff8E/3//BP9/"
        "AAf/fP8H/nz/CP98/ws/ngMO/3kDEv14Awj9YQL//GEEQ/hsBGn4agRw9HoGdORefkzjX"
        "35k5z9/POf4zzrQM9Uyz+SvOLsYkx8SMYXPgFkeMPlG5g0Pb/X8YISJbh4HpL8dRpGvQU"
        "Mp+xcQ=="
    )
    try:
        st.markdown(
            f"""
            <audio autoplay hidden>
                <source src="data:audio/wav;base64,{BEEP_BASE64}" type="audio/wav">
            </audio>
            """,
            unsafe_allow_html=True,
        )
    except Exception:
        import base64 as _b64
        audio_bytes = _b64.b64decode(BEEP_BASE64)
        st.audio(audio_bytes, format="audio/wav")


def send_email_notification(to_address: str, subject: str, message: str) -> None:
    """Stub de mail. En producción, implementar con smtplib o servicio externo."""
    st.info(f"Correo enviado a {to_address} – {subject}")

# ---------------------------------------------------------------------------
# Usuarios demo (en producción: hashes de contraseña)
USERS: Dict[str, Dict[str, str]] = {
    "mantenimiento": {"password": "1234", "role": "Mantenimiento"},
    "operaciones": {"password": "1234", "role": "Terminales"},
}

LOGO_PATH = os.path.join(os.path.dirname(__file__), "descarga.png")

# Ruta antigua local (ya no se usa para persistir, queda por compatibilidad)
DATA_FILE = os.path.join(os.path.dirname(__file__), "maintenance_data.json")

# ---------------------------------------------------------------------------

def init_state() -> None:
    """Inicializa el estado (flota, inventario, scheduler, etc.) y carga del repo HF."""
    if "fleet" not in st.session_state:
        st.session_state.fleet: Dict[str, Equipment] = {}
        st.session_state.inventory = Inventory()
        st.session_state.scheduler = Scheduler(
            st.session_state.fleet, st.session_state.inventory
        )
        st.session_state.failure_log = FailureLog()
        st.session_state.work_requests: List[Dict] = []
        st.session_state.notifications_ops: List[str] = []

        st.session_state.default_components = {
            "Amortiguadores": Component(
                "Amortiguadores", "alta", hours_interval=500, km_interval=50000, days_interval=365
            ),
            "Limpiaparabrisas": Component(
                "Limpiaparabrisas", "alta", hours_interval=200, km_interval=None, days_interval=180
            ),
            "Luces": Component(
                "Luces", "alta", hours_interval=None, km_interval=None, days_interval=90
            ),
        }

        # Repuestos demo
        st.session_state.inventory.add_part(
            "Amortiguador delantero", initial_stock=10, min_stock=2, fits_components=["Amortiguadores"]
        )
        st.session_state.inventory.add_part(
            "Plumillas limpiaparabrisas", initial_stock=20, min_stock=5, fits_components=["Limpiaparabrisas"]
        )
        st.session_state.inventory.add_part(
            "Foco delantero", initial_stock=30, min_stock=5, fits_components=["Luces"]
        )

        # Categorías de componentes
        st.session_state.component_categories = {
            "Suspensión": ["Amortiguadores"],
            "Cabina": ["Limpiaparabrisas", "Luces"],
            "Motor": [],
            "Transmisión": [],
            "Tren delantero": [],
            "Tren trasero": [],
            "Frenos": [],
            "Otros": [],
        }

        # Semilla de tractos
        default_ids = [
            "T648", "T659", "T779", "T789", "T73", "T74",
            "K69", "K71", "K72", "K73", "K75", "K76",
            "M01", "M02", "M03", "M04",
        ]
        for eq_id in default_ids:
            if eq_id.startswith("T"):
                brand = "Terberg"
            elif eq_id.startswith("K"):
                brand = "Kalmar"
            elif eq_id.startswith("M"):
                brand = "MOL"
            else:
                brand = "Tracto"
            description = f"Tracto {brand}"
            if eq_id not in st.session_state.fleet:
                new_eq = Equipment(eq_id, description)
                for comp in st.session_state.default_components.values():
                    new_eq.register_component(comp)
                st.session_state.fleet[eq_id] = new_eq

    if "work_requests" not in st.session_state:
        st.session_state.work_requests = []
    if "notifications_ops" not in st.session_state:
        st.session_state.notifications_ops = []
    if "last_notif_count_ops" not in st.session_state:
        st.session_state.last_notif_count_ops = 0
    if "last_notif_count_mtto" not in st.session_state:
        st.session_state.last_notif_count_mtto = 0

    # Cargar del dataset repo (persistencia real)
    try:
        load_data()
    except Exception:
        pass

# ---------------------------------------------------------------------------

def add_equipment_form() -> None:
    with st.form(key="add_equipment_form"):
        eq_id = st.text_input("ID del equipo", value="T")
        eq_desc = st.text_input("Descripción", value="Tracto")
        submitted = st.form_submit_button("Añadir equipo")
        if submitted:
            if not eq_id:
                st.error("El ID no puede estar vacío")
            elif eq_id in st.session_state.fleet:
                st.error(f"Ya existe un equipo con ID {eq_id}")
            else:
                new_eq = Equipment(eq_id, eq_desc)
                for comp in st.session_state.default_components.values():
                    new_eq.register_component(comp)
                st.session_state.fleet[eq_id] = new_eq
                save_data("alta equipo")
                st.success(f"Equipo {eq_id} registrado correctamente")


def update_readings_form() -> None:
    if not st.session_state.fleet:
        st.info("No hay equipos registrados.")
        return
    eq_ids = list(st.session_state.fleet.keys())
    selected = st.selectbox("Seleccionar equipo", eq_ids, key="upd_sel_eq")
    eq = st.session_state.fleet[selected]
    st.write(
        f"Horómetro actual: {eq.horometro:.1f} h | Odómetro actual: {eq.odometro:.1f} km"
    )
    add_hours = st.number_input(
        "Horas adicionales", min_value=0.0, step=0.5, key="upd_hours"
    )
    add_km = st.number_input(
        "Kilómetros adicionales", min_value=0.0, step=0.5, key="upd_km"
    )
    if st.button("Actualizar lecturas", key="upd_btn"):
        eq.update_horometro(add_hours)
        eq.update_odometro(add_km)
        save_data("update lecturas")
        st.success(
            f"Equipo {eq.id}: horómetro +{add_hours} h, odómetro +{add_km} km"
        )


def fleet_summary() -> Tuple[int, int, int, int]:
    total = len(st.session_state.fleet)
    available = 0
    in_maintenance = 0
    due_soon = 0
    horizon = datetime.date.today() + datetime.timedelta(days=7)
    pending_req_ids = set(
        req["equipment_id"]
        for req in st.session_state.work_requests
        if req.get("status") == "pendiente"
    )
    for eq in st.session_state.fleet.values():
        if eq.status == "operativo" and eq.id not in pending_req_ids:
            available += 1
        if eq.status == "en mantenimiento":
            in_maintenance += 1
    for order in st.session_state.scheduler.pending_orders:
        if order.status == "pendiente" and order.due_date <= horizon:
            due_soon += 1
    return total, available, due_soon, in_maintenance


def render_calendar(pending_orders: List[WorkOrder]) -> str:
    today = datetime.date.today()
    year, month = today.year, today.month
    tasks_by_day: Dict[int, int] = {}
    for order in pending_orders:
        if (
            order.status == "pendiente"
            and order.due_date.year == year
            and order.due_date.month == month
        ):
            tasks_by_day[order.due_date.day] = tasks_by_day.get(order.due_date.day, 0) + 1
    cal = calendar.Calendar(firstweekday=0)
    weeks = cal.monthdayscalendar(year, month)
    html = [
        '<table style="border-collapse: collapse; width: 100%; text-align: center;">',
        f'<caption style="margin-bottom: 8px; font-weight: bold;">{calendar.month_name[month]} {year}</caption>',
        '<thead><tr>'
    ]
    for day_name in ["Lu", "Ma", "Mi", "Ju", "Vi", "Sa", "Do"]:
        html.append(
            f'<th style="border: 1px solid #ddd; padding: 4px; background-color: #f5f5f5;">{day_name}</th>'
        )
    html.append('</tr></thead><tbody>')
    for week in weeks:
        html.append('<tr>')
        for day in week:
            if day == 0:
                html.append('<td style="border: 1px solid #ddd; padding: 8px; height: 60px;"></td>')
            else:
                count = tasks_by_day.get(day)
                if count:
                    cell_style = (
                        "border: 1px solid #ddd; padding: 4px; height: 60px; "
                        "background-color: #f8d7da; color: #721c24;"
                    )
                    content = f'<strong>{day}</strong><br/><span style="font-size: 0.8em;">{count} OT</span>'
                else:
                    cell_style = "border: 1px solid #ddd; padding: 4px; height: 60px;"
                    content = f"{day}"
                html.append(f'<td style="{cell_style}">{content}</td>')
        html.append('</tr>')
    html.append('</tbody></table>')
    return "".join(html)


def serialize_session_state() -> Dict[str, object]:
    data: Dict[str, object] = {}
    fleet_data = {}
    for eq_id, eq in st.session_state.fleet.items():
        components_data = []
        for rec in eq.components.values():
            comp = rec.component
            components_data.append({
                "name": comp.name,
                "criticidad": comp.criticidad,
                "hours_interval": comp.hours_interval,
                "km_interval": comp.km_interval,
                "days_interval": comp.days_interval,
                "last_service_date": rec.last_service_date.isoformat(),
                "last_service_hours": rec.last_service_hours,
                "last_service_km": rec.last_service_km,
            })
        fleet_data[eq_id] = {
            "description": eq.description,
            "horometro": eq.horometro,
            "odometro": eq.odometro,
            "status": eq.status,
            "components": components_data,
        }
    data["fleet"] = fleet_data
    inv_data = {}
    for part_name, (qty, min_qty) in st.session_state.inventory._stock.items():
        inv_data[part_name] = {
            "stock": qty,
            "min_stock": min_qty,
            "fits_components": st.session_state.inventory._part_mapping.get(part_name, []),
        }
    data["inventory"] = inv_data
    orders_data = []
    for order in st.session_state.scheduler.pending_orders:
        orders_data.append({
            "id": order.id,
            "equipment_id": order.equipment_id,
            "component_name": order.component_name,
            "due_date": order.due_date.isoformat(),
            "reason": order.reason,
            "classification": getattr(order, "classification", ""),
            "materials_used": getattr(order, "materials_used", []),
            "status": order.status,
            "created_at": order.created_at.isoformat(),
            "completed_at": (
                getattr(order, "completed_at", None).isoformat() if getattr(order, "completed_at", None) else None
            ),
            "start_time": (
                getattr(order, "start_time", None).isoformat() if getattr(order, "start_time", None) else None
            ),
        })
    data["pending_orders"] = orders_data
    requests_data = []
    for req in st.session_state.work_requests:
        requests_data.append({
            **req,
            "date": req["date"].isoformat() if isinstance(req["date"], datetime.date) else req["date"],
            "created_at": req["created_at"].isoformat() if isinstance(req.get("created_at"), datetime.datetime) else req.get("created_at"),
        })
    data["work_requests"] = requests_data
    failures_data = []
    for entry in st.session_state.failure_log.entries:
        ts, eq_id, comp, desc, repair = entry
        failures_data.append({
            "timestamp": ts.isoformat(),
            "equipment_id": eq_id,
            "component_name": comp,
            "description": desc,
            "repair_time_hours": repair,
        })
    data["failures"] = failures_data
    data["notifications_ops"] = list(st.session_state.notifications_ops)
    data["users"] = USERS
    return data


# -------------------- PERSISTENCIA HF (reemplaza persistencia local) --------------------

def save_data(message: str = "update") -> None:
    """Guarda estado en el dataset repo de Hugging Face (commit+push)."""
    try:
        data = serialize_session_state()
        hf_save_state(data, message=message)
    except Exception as e:
        st.warning(f"No se pudo guardar en el repo HF: {e}")


def load_data() -> None:
    """Carga estado desde el dataset repo de Hugging Face y reconstruye session_state."""
    try:
        data = hf_load_state()
    except Exception as e:
        st.warning(f"No se pudo leer del repo HF: {e}")
        return

    if not data:
        return

    # Flota
    fleet_data = data.get("fleet", {})
    st.session_state.fleet = {}
    for eq_id, eq_info in fleet_data.items():
        eq = Equipment(eq_id, eq_info.get("description", "Tracto"))
        eq.horometro = eq_info.get("horometro", 0.0)
        eq.odometro = eq_info.get("odometro", 0.0)
        eq.status = eq_info.get("status", "operativo")
        for comp_info in eq_info.get("components", []):
            comp = Component(
                comp_info["name"],
                comp_info.get("criticidad", "media"),
                comp_info.get("hours_interval"),
                comp_info.get("km_interval"),
                comp_info.get("days_interval"),
            )
            last_date = datetime.date.fromisoformat(comp_info["last_service_date"])
            last_hours = comp_info.get("last_service_hours", 0.0)
            last_km = comp_info.get("last_service_km", 0.0)
            eq.register_component(comp, service_date=last_date, service_hours=last_hours, service_km=last_km)
        st.session_state.fleet[eq_id] = eq

    # Inventario
    inv_data = data.get("inventory", {})
    st.session_state.inventory = Inventory()
    for part_name, info in inv_data.items():
        st.session_state.inventory.add_part(
            part_name,
            info.get("stock", 0),
            info.get("min_stock", 0),
            info.get("fits_components", []),
        )

    # Scheduler y OTs
    st.session_state.scheduler = Scheduler(st.session_state.fleet, st.session_state.inventory)
    st.session_state.scheduler.pending_orders.clear()
    for od in data.get("pending_orders", []):
        wo = WorkOrder(
            equipment_id=od["equipment_id"],
            component_name=od["component_name"],
            due_date=datetime.date.fromisoformat(od["due_date"]),
            reason=od.get("reason", ""),
            classification=od.get("classification", ""),
        )
        wo.id = od.get("id", wo.id)
        wo.materials_used = od.get("materials_used", [])
        wo.status = od.get("status", "pendiente")
        # reconstruye times si existen
        if od.get("created_at"):
            try:
                wo.created_at = datetime.datetime.fromisoformat(od["created_at"])
            except Exception:
                pass
        if od.get("completed_at"):
            try:
                wo.completed_at = datetime.datetime.fromisoformat(od["completed_at"])
            except Exception:
                pass
        if od.get("start_time"):
            try:
                wo.start_time = datetime.datetime.fromisoformat(od["start_time"])
            except Exception:
                pass
        st.session_state.scheduler.pending_orders.append(wo)

    # Solicitudes
    st.session_state.work_requests = []
    for req in data.get("work_requests", []):
        req_copy = dict(req)
        if req_copy.get("date"):
            try:
                req_copy["date"] = datetime.date.fromisoformat(req_copy["date"])
            except ValueError:
                pass
        if req_copy.get("created_at"):
            try:
                req_copy["created_at"] = datetime.datetime.fromisoformat(req_copy["created_at"])
            except Exception:
                pass
        st.session_state.work_requests.append(req_copy)

    # Fallas
    st.session_state.failure_log = FailureLog()
    for entry in data.get("failures", []):
        try:
            ts = datetime.datetime.fromisoformat(entry["timestamp"])
        except Exception:
            ts = datetime.datetime.now()
        st.session_state.failure_log.entries.append(
            (ts, entry["equipment_id"], entry["component_name"], entry["description"], entry["repair_time_hours"])
        )

    # Notificaciones y usuarios
    st.session_state.notifications_ops = data.get("notifications_ops", [])
    users_data = data.get("users")
    if users_data:
        for uname, info in users_data.items():
            USERS[uname] = info

# ---------------------------------------------------------------------------

def display_dashboard() -> None:
    total, available, due_soon, in_maintenance = fleet_summary()
    # métrica de disponibilidad (%)
    availability_pct = (available / total * 100) if total else 0.0

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total equipos", total)
    col2.metric("Disponibles", available)
    col3.metric("Disponibilidad (%)", f"{availability_pct:.1f}")
    col4.metric("Próx. a mantenimiento", due_soon)
    col5.metric("En mantenimiento", in_maintenance)

    if st.session_state.fleet:
        horizon = datetime.date.today() + datetime.timedelta(days=7)
        due_soon_eq = {
            order.equipment_id
            for order in st.session_state.scheduler.pending_orders
            if order.status == "pendiente" and order.due_date <= horizon
        }
        fail_eq = {
            req["equipment_id"]
            for req in st.session_state.work_requests
            if req.get("status") == "pendiente" and req.get("classification") == "alta"
        }
        pending_all = {
            req["equipment_id"]
            for req in st.session_state.work_requests
            if req.get("status") == "pendiente"
        }
        solicit_eq = pending_all - fail_eq
        rows = []
        for eq in st.session_state.fleet.values():
            if eq.id in fail_eq:
                icon = "🚨"; label = "Falla"
            elif eq.status == "en mantenimiento":
                icon = "🔧"; label = "Mantenimiento"
            elif eq.id in solicit_eq:
                icon = "🟠"; label = "Solicitud"
            elif eq.id in due_soon_eq:
                icon = "🟡"; label = "Próx. mant."
            else:
                icon = "🟢"; label = "Operativo"
            rows.append(
                {
                    "ID": eq.id,
                    "Descripción": eq.description,
                    "Horómetro (h)": f"{eq.horometro:.1f}",
                    "Odómetro (km)": f"{eq.odometro:.1f}",
                    "Estado": f"{icon} {label}",
                }
            )
        st.dataframe(rows, use_container_width=True)
    else:
        st.info("No hay equipos registrados.")

    calendar_html = render_calendar(st.session_state.scheduler.pending_orders)
    st.markdown(calendar_html, unsafe_allow_html=True)

# ---------------------------------------------------------------------------

def operations_view() -> None:
    st.header("Terminales / Operaciones")

    if st.button("Refrescar ahora", key="ops_refresh_btn"):
        st.experimental_rerun()

    try:
        from streamlit_autorefresh import st_autorefresh  # type: ignore
        st_autorefresh(interval=5000, key="ops_autorefresh")
    except Exception:
        pass

    tabs = st.tabs(
        [
            "Resumen",
            "Crear OT",
            "Seguimiento OT",
            "Checklist diario",
            "Historial de fallas",
            "Disponibilidad",
        ]
    )

    with tabs[0]:
        st.subheader("Resumen de flota")
        display_dashboard()
        notif_list = st.session_state.notifications_ops
        if notif_list:
            new_count = len(notif_list)
            if new_count > st.session_state.last_notif_count_ops:
                play_alert_sound()
            st.session_state.last_notif_count_ops = new_count
            st.subheader("Notificaciones recientes de mantenimiento")
            for msg in notif_list:
                st.info(msg)
            if st.button("Marcar como leídas", key="ops_notif_read"):
                st.session_state.notifications_ops = []
                st.session_state.last_notif_count_ops = 0
                save_data("ops lee notifs")

    with tabs[1]:
        st.subheader("Crear solicitud de mantenimiento")
        if not st.session_state.fleet:
            st.info("Debe registrar al menos un equipo para crear solicitudes.")
        else:
            req_id = uuid.uuid4().hex
            eq_sel = st.selectbox("Equipo", list(st.session_state.fleet.keys()), key="ops_req_eq")
            categories = list(st.session_state.component_categories.keys())
            system_sel = st.selectbox("Sistema", categories, key="ops_req_sys")
            comp_options = st.session_state.component_categories.get(system_sel, [])
            if comp_options:
                comp_sel = st.selectbox("Componente", comp_options, key="ops_req_comp")
            else:
                comp_sel = st.text_input("Componente", key="ops_req_comp_text")
            classification = st.selectbox("Criticidad", ["alta", "media", "baja"], key="ops_req_class")
            comments = st.text_area("Comentarios / descripción de la falla", key="ops_req_comments")
            horometro = st.number_input("Lectura actual de horómetro (h)", min_value=0.0, step=0.5, key="ops_req_hr")
            date_report = st.date_input("Fecha de reporte", value=datetime.date.today(), key="ops_req_date")
            photo = st.file_uploader("Adjuntar foto (opcional)", type=["jpg", "jpeg", "png"], key="ops_req_photo")
            if st.button("Enviar solicitud", key="ops_req_submit"):
                req = {
                    "id": req_id,
                    "equipment_id": eq_sel,
                    "component_name": comp_sel,
                    "classification": classification,
                    "comments": comments,
                    "photo_name": photo.name if photo else None,
                    "horometro": horometro,
                    "date": date_report,
                    "status": "pendiente",
                    "created_at": datetime.datetime.now(),
                }
                st.session_state.work_requests.append(req)
                eq_obj = st.session_state.fleet.get(eq_sel)
                if eq_obj and eq_obj.status == "operativo":
                    eq_obj.set_status("en solicitud")
                save_data("ops crea solicitud")
                st.success(f"Solicitud enviada (ID {req_id})")

    with tabs[2]:
        st.subheader("Seguimiento de solicitudes y órdenes")
        if st.session_state.work_requests:
            req_rows = []
            for req in st.session_state.work_requests:
                req_rows.append(
                    {
                        "ID solicitud": req["id"],
                        "Equipo": req["equipment_id"],
                        "Componente": req["component_name"],
                        "Criticidad": req["classification"],
                        "Fecha": req["date"],
                        "Estado": req["status"],
                    }
                )
            st.write("Solicitudes")
            st.table(req_rows)
        else:
            st.info("No hay solicitudes registradas.")

        if st.session_state.scheduler.pending_orders:
            ot_rows = []
            for ot in st.session_state.scheduler.pending_orders:
                ot_rows.append(
                    {
                        "ID OT": ot.id,
                        "Equipo": ot.equipment_id,
                        "Componente": ot.component_name,
                        "Criticidad": getattr(ot, "classification", ""),
                        "Fecha programa": ot.due_date,
                        "Estado": ot.status,
                    }
                )
            st.write("Órdenes de trabajo")
            st.table(ot_rows)
        else:
            st.info("No hay órdenes de trabajo registradas.")

    with tabs[3]:
        st.subheader("Checklist diario de horómetro y odómetro")
        update_readings_form()

    with tabs[4]:
        st.subheader("Historial de fallas")
        if not st.session_state.failure_log.entries:
            st.info("No hay fallas registradas.")
        else:
            eq_choices = list({entry[1] for entry in st.session_state.failure_log.entries})
            eq_choices.sort()
            eq_filter = st.selectbox("Equipo", ["Todos"] + eq_choices, key="ops_fail_filter")
            fail_rows = []
            for ts, eq_id, comp, desc, repair_h in st.session_state.failure_log.entries:
                if eq_filter != "Todos" and eq_id != eq_filter:
                    continue
                fail_rows.append(
                    {
                        "Fecha": ts.strftime("%Y-%m-%d %H:%M"),
                        "Equipo": eq_id,
                        "Componente": comp,
                        "Descripción": desc,
                        "Horas reparación": repair_h,
                    }
                )
            if fail_rows:
                st.table(fail_rows)
            else:
                st.info("No hay fallas para el equipo seleccionado.")

    with tabs[5]:
        st.subheader("Reporte de disponibilidad de la flota")
        total, available, due_soon, in_maintenance = fleet_summary()
        fail_eq = {
            req["equipment_id"]
            for req in st.session_state.work_requests
            if req["status"] == "pendiente" and req["classification"] == "alta"
        }
        failure = len(fail_eq)
        counts = {
            "Disponible": available,
            "En mantenimiento": in_maintenance,
            "Próx. mantenimiento": due_soon,
            "Falla": failure,
        }
        df_counts = pd.DataFrame.from_dict(counts, orient="index", columns=["Cantidad"])
        st.bar_chart(df_counts)

# ---------------------------------------------------------------------------

def process_work_requests() -> None:
    if not st.session_state.work_requests:
        st.info("No hay solicitudes de mantenimiento pendientes.")
        return
    for req in list(st.session_state.work_requests):
        if req["status"] != "pendiente":
            continue
        with st.expander(
            f"Solicitud {req['id']} – Equipo {req['equipment_id']} ({req['component_name']})"
        ):
            st.write(f"Fecha reporte: {req['date']}")
            st.write(f"Lectura horómetro: {req['horometro']} h")
            st.write(f"Criticidad sugerida: {req['classification']}")
            st.write(f"Comentarios: {req['comments']}")
            if req.get("photo_name"):
                st.write(f"Adjunto: {req['photo_name']}")
            new_class = st.selectbox(
                "Asignar criticidad",
                ["alta", "media", "baja"],
                index=["alta", "media", "baja"].index(req["classification"]),
                key=f"reclass_{req['id']}"
            )
            due_date = st.date_input(
                "Programar para (fecha)", value=datetime.date.today(), key=f"due_{req['id']}"
            )
            due_time_str = st.text_input(
                "Programar para (hora HH:MM)", value="08:00", key=f"due_time_{req['id']}"
            )
            if st.button("Convertir a OT", key=f"conv_{req['id']}"):
                reason = f"Solicitud de operaciones: {req['comments']}"
                try:
                    due_time = datetime.datetime.strptime(due_time_str, "%H:%M").time()
                except ValueError:
                    due_time = datetime.time(0, 0)
                    st.warning("Formato de hora inválido (HH:MM)")
                ot = WorkOrder(
                    equipment_id=req["equipment_id"],
                    component_name=req["component_name"],
                    due_date=due_date,
                    reason=reason,
                )
                try:
                    setattr(ot, "due_time", due_time)
                except Exception:
                    pass
                try:
                    ot.classification = new_class
                except Exception:
                    pass
                st.session_state.scheduler.pending_orders.append(ot)
                equipment = st.session_state.fleet.get(req["equipment_id"])
                if equipment:
                    equipment.set_status("en mantenimiento")
                try:
                    desc = req.get("comments", "Falla reportada por operaciones")
                    st.session_state.failure_log.log_failure(
                        req["equipment_id"], req["component_name"], desc, 0.0
                    )
                except Exception:
                    pass
                req["status"] = "procesada"
                req["classification"] = new_class
                st.session_state.notifications_ops.append(
                    f"Solicitud {req['id']} convertida en OT {ot.id} con criticidad '{new_class}'"
                )
                save_data("convierte solicitud a OT")
                st.success(f"OT {ot.id} creada a partir de la solicitud {req['id']}")

def manage_orders() -> None:
    pending = [o for o in st.session_state.scheduler.pending_orders if o.status == "pendiente"]
    if not pending:
        st.info("No hay órdenes pendientes.")
        return
    for ot in pending:
        with st.expander(f"OT {ot.id} – Equipo {ot.equipment_id} ({ot.component_name})"):
            st.write(f"Creada: {ot.created_at.date()}")
            st.write(f"Programada para: {ot.due_date}")
            st.write(f"Razón: {ot.reason}")
            current_class = getattr(ot, "classification", "")
            st.write(f"Criticidad actual: {current_class}")
            new_class = st.selectbox(
                "Modificar criticidad",
                ["alta", "media", "baja"],
                index=["alta", "media", "baja"].index(getattr(ot, "classification", "alta") or "alta"),
                key=f"edit_class_{ot.id}"
            )
            new_due = st.date_input("Modificar fecha programada", value=ot.due_date, key=f"edit_due_{ot.id}")
            if st.button("Guardar cambios", key=f"save_ot_{ot.id}"):
                try:
                    ot.classification = new_class
                except Exception:
                    pass
                ot.due_date = new_due
                st.session_state.notifications_ops.append(
                    f"OT {ot.id} reclasificada a '{new_class}' y reprogramada"
                )
                save_data("edita OT")
                st.success(f"OT {ot.id} actualizada")

            default_start_time = getattr(ot, "start_time", None)
            default_end_time = getattr(ot, "completed_at", None)
            start_def_time = default_start_time.time() if default_start_time else datetime.time(0, 0)
            end_def_time = default_end_time.time() if default_end_time else datetime.time(0, 0)

            start_time_str = st.text_input(
                "Hora de inicio (HH:MM)",
                value=start_def_time.strftime("%H:%M"),
                key=f"start_time_str_{ot.id}"
            )
            try:
                start_time = datetime.datetime.strptime(start_time_str, "%H:%M").time()
            except ValueError:
                start_time = start_def_time
                st.warning("Formato de hora de inicio inválido. Use HH:MM")

            end_time_str = st.text_input(
                "Hora de término (HH:MM)",
                value=end_def_time.strftime("%H:%M"),
                key=f"end_time_str_{ot.id}"
            )
            try:
                end_time = datetime.datetime.strptime(end_time_str, "%H:%M").time()
            except ValueError:
                end_time = end_def_time
                st.warning("Formato de hora de término inválido. Use HH:MM")

            current_mats = getattr(ot, "materials_used", [])
            materials = st.text_input(
                "Materiales utilizados (separados por comas)",
                value=", ".join(current_mats),
                key=f"mat_{ot.id}"
            )
            comments = st.text_input(
                "Comentarios (opcional)",
                value=getattr(ot, "comments", ""),
                key=f"comments_{ot.id}"
            )
            if st.button("Marcar completada", key=f"comp_ot_{ot.id}"):
                used = [m.strip() for m in materials.split(",") if m.strip()]
                try:
                    setattr(ot, "materials_used", used)
                except Exception:
                    pass
                try:
                    setattr(ot, "comments", comments)
                except Exception:
                    pass
                start_dt = datetime.datetime.combine(ot.due_date, start_time)
                end_dt = datetime.datetime.combine(ot.due_date, end_time)
                if end_dt < start_dt:
                    end_dt = start_dt
                ot.status = "en progreso"
                ot.start_time = start_dt
                ot.completed_at = end_dt
                ot.status = "completada"
                eq = st.session_state.fleet.get(ot.equipment_id)
                if eq:
                    eq.set_status("operativo")
                st.session_state.scheduler.complete_order(ot.id)
                start_str = ot.start_time.strftime("%Y-%m-%d %H:%M") if ot.start_time else "—"
                end_str = ot.completed_at.strftime("%Y-%m-%d %H:%M") if ot.completed_at else "—"
                mats = ", ".join(getattr(ot, "materials_used", [])) if getattr(ot, "materials_used", []) else "N/A"
                st.session_state.notifications_ops.append(
                    f"OT {ot.id} completada (inicio: {start_str}, fin: {end_str}, materiales: {mats})"
                )
                save_data("completa OT")
                st.success(f"OT {ot.id} completada")

def mechanic_orders() -> None:
    pending = [o for o in st.session_state.scheduler.pending_orders if o.status == "pendiente"]
    if not pending:
        st.info("No hay órdenes pendientes.")
        return
    for ot in pending:
        with st.expander(f"OT {ot.id} – Equipo {ot.equipment_id} ({ot.component_name})"):
            st.write(f"Creada: {ot.created_at.date()}")
            st.write(f"Programada para: {ot.due_date}")
            st.write(f"Razón: {ot.reason}")
            st.write(f"Criticidad: {getattr(ot, 'classification', '')}")

            default_start_time = getattr(ot, "start_time", None)
            default_end_time = getattr(ot, "completed_at", None)
            start_def_time = default_start_time.time() if default_start_time else datetime.time(0, 0)
            end_def_time = default_end_time.time() if default_end_time else datetime.time(0, 0)

            start_time_str = st.text_input(
                "Hora de inicio (HH:MM)",
                value=start_def_time.strftime("%H:%M"),
                key=f"m_start_str_{ot.id}"
            )
            try:
                start_time = datetime.datetime.strptime(start_time_str, "%H:%M").time()
            except ValueError:
                start_time = start_def_time
                st.warning("Formato de hora de inicio inválido. Use HH:MM")

            end_time_str = st.text_input(
                "Hora de término (HH:MM)",
                value=end_def_time.strftime("%H:%M"),
                key=f"m_end_str_{ot.id}"
            )
            try:
                end_time = datetime.datetime.strptime(end_time_str, "%H:%M").time()
            except ValueError:
                end_time = end_def_time
                st.warning("Formato de hora de término inválido. Use HH:MM")

            current_mats = getattr(ot, "materials_used", [])
            materials = st.text_input(
                "Materiales utilizados (separados por comas)",
                value=", ".join(current_mats),
                key=f"m_mat_{ot.id}"
            )
            comments = st.text_input(
                "Comentarios (opcional)",
                value=getattr(ot, "comments", ""),
                key=f"m_comments_{ot.id}"
            )
            if st.button("Cerrar trabajo", key=f"m_comp_{ot.id}"):
                used = [m.strip() for m in materials.split(",") if m.strip()]
                setattr(ot, "materials_used", used)
                setattr(ot, "comments", comments)
                start_dt = datetime.datetime.combine(ot.due_date, start_time)
                end_dt = datetime.datetime.combine(ot.due_date, end_time)
                if end_dt < start_dt:
                    end_dt = start_dt
                ot.status = "en progreso"
                ot.start_time = start_dt
                ot.completed_at = end_dt
                ot.status = "completada"
                eq = st.session_state.fleet.get(ot.equipment_id)
                if eq:
                    eq.set_status("operativo")
                st.session_state.scheduler.complete_order(ot.id)
                start_str = ot.start_time.strftime("%Y-%m-%d %H:%M") if ot.start_time else "—"
                end_str = ot.completed_at.strftime("%Y-%m-%d %H:%M") if ot.completed_at else "—"
                mats = ", ".join(getattr(ot, "materials_used", [])) if getattr(ot, "materials_used", []) else "N/A"
                st.session_state.notifications_ops.append(
                    f"OT {ot.id} completada (inicio: {start_str}, fin: {end_str}, materiales: {mats})"
                )
                save_data("cierra trabajo mecánico")
                st.success(f"Trabajo para OT {ot.id} completado")

def schedule_automatic_maintenance() -> None:
    if st.button("Verificar órdenes programadas"):
        new_orders = st.session_state.scheduler.check_due_maintenance()
        if new_orders:
            save_data("genera OT programadas")
            st.success(f"Se generaron {len(new_orders)} OT programadas.")
        else:
            st.info("No se generaron nuevas OT programadas.")

def manual_order_form() -> None:
    if not st.session_state.fleet:
        st.info("No hay equipos disponibles.")
        return
    eq_sel = st.selectbox("Equipo", list(st.session_state.fleet.keys()), key="manual_ot_eq")
    categories = list(st.session_state.component_categories.keys())
    system_sel = st.selectbox("Sistema", categories, key="manual_ot_sys")
    comp_options = st.session_state.component_categories.get(system_sel, [])
    if comp_options:
        comp_sel = st.selectbox("Componente", comp_options, key="manual_ot_comp")
    else:
        comp_sel = st.text_input("Componente", key="manual_ot_comp_text")
    classification = st.selectbox("Criticidad", ["alta", "media", "baja"], key="manual_ot_class")
    due_date = st.date_input("Programar para (fecha)", value=datetime.date.today(), key="manual_ot_due")
    due_time_str = st.text_input("Programar para (hora HH:MM)", value="08:00", key="manual_ot_due_time")
    reason = st.text_input("Motivo de la OT", key="manual_ot_reason")
    if st.button("Crear OT", key="manual_ot_submit"):
        try:
            due_time = datetime.datetime.strptime(due_time_str, "%H:%M").time()
        except ValueError:
            due_time = datetime.time(0, 0)
            st.warning("Formato de hora inválido (HH:MM)")
        ot = WorkOrder(
            equipment_id=eq_sel,
            component_name=comp_sel,
            due_date=due_date,
            reason=reason or "OT programada manualmente",
        )
        try:
            setattr(ot, "due_time", due_time)
        except Exception:
            pass
        try:
            ot.classification = classification
        except Exception:
            pass
        st.session_state.scheduler.pending_orders.append(ot)
        eq_obj = st.session_state.fleet.get(eq_sel)
        if eq_obj:
            eq_obj.set_status("en mantenimiento")
        st.session_state.notifications_ops.append(
            f"OT {ot.id} creada manualmente para equipo {eq_sel} con criticidad '{classification}'"
        )
        save_data("crea OT manual")
        st.success(f"OT {ot.id} creada")

# ---------------------------------------------------------------------------

def maintenance_view() -> None:
    st.header("Mantenimiento")

    if st.button("Refrescar ahora", key="mtto_refresh_btn"):
        st.experimental_rerun()
    try:
        from streamlit_autorefresh import st_autorefresh  # type: ignore
        st_autorefresh(interval=5000, key="mtto_autorefresh")
    except Exception:
        pass

    tabs = st.tabs(
        [
            "Resumen",
            "Solicitudes",
            "Programación automática",
            "Crear OT",
            "Órdenes",
            "Trabajos",
            "Inventario & Flota",
            "Registro de fallas",
            "Métricas & Confiabilidad",
            "Disponibilidad",
        ]
    )

    with tabs[0]:
        st.subheader("Resumen de flota")
        display_dashboard()
        pending_reqs = sum(1 for r in st.session_state.work_requests if r.get("status") == "pendiente")
        pending_ots = len(st.session_state.scheduler.pending_orders)
        current_count = pending_reqs + pending_ots
        if current_count > st.session_state.last_notif_count_mtto:
            play_alert_sound()
            diff = current_count - st.session_state.last_notif_count_mtto
            st.warning(
                f"Se han recibido {diff} nuevas solicitudes u órdenes de trabajo. "
                "Revise las pestañas de Solicitudes u Órdenes para más detalles."
            )
        st.session_state.last_notif_count_mtto = current_count

        # Exportar base de datos desde el dashboard
        st.markdown("---")
        st.subheader("Descargar base de datos")
        try:
            files = export_csv_parquet(serialize_session_state(), out_dir="exports")
            with open(files["equipos"], "rb") as f:
                st.download_button(
                    "Descargar equipos.csv",
                    data=f,
                    file_name="equipos.csv",
                    mime="text/csv",
                )
        except Exception as e:
            st.warning(f"No se pudo generar la exportación: {e}")

    with tabs[1]:
        st.subheader("Solicitudes pendientes de operaciones")
        process_work_requests()

    with tabs[2]:
        st.subheader("Programación automática por horómetro/kilometraje/tiempo")
        schedule_automatic_maintenance()

    with tabs[3]:
        st.subheader("Crear OT programada manualmente")
        manual_order_form()

    with tabs[4]:
        st.subheader("Órdenes de trabajo pendientes")
        manage_orders()

    with tabs[5]:
        st.subheader("Trabajos en ejecución (mecánicos)")
        mechanic_orders()

    with tabs[6]:
        st.subheader("Inventario de repuestos")
        inventory = st.session_state.inventory
        if inventory._stock:
            rows = []
            for part_name, (qty, min_qty) in inventory._stock.items():
                rows.append({"Repuesto": part_name, "Stock": qty, "Mínimo": min_qty})
            st.table(rows)
            low = inventory.low_stock_alerts()
            if low:
                st.warning(
                    "Repuestos con stock bajo: "
                    + ", ".join([f"{p} (stock {inventory.get_stock(p)})" for p in low])
                )
        else:
            st.info("No hay repuestos registrados.")
        with st.form(key="add_part_form_roles"):
            st.write("Añadir nuevo repuesto")
            name = st.text_input("Nombre del repuesto", key="add_part_name")
            initial = st.number_input("Stock inicial", min_value=0, step=1, value=0, key="add_part_initial")
            min_stock = st.number_input("Stock mínimo", min_value=0, step=1, value=0, key="add_part_min")
            submitted = st.form_submit_button("Añadir repuesto")
            if submitted and name:
                inventory.add_part(name, initial, min_stock, [])
                save_data("alta repuesto")
                st.success(f"Repuesto '{name}' añadido al inventario")
        add_equipment_form()

    with tabs[7]:
        st.subheader("Registro de fallas")
        fl = st.session_state.failure_log
        with st.form(key="fail_log_roles"):
            st.write("Registrar nueva falla")
            if not st.session_state.fleet:
                st.info("No hay equipos disponibles")
            else:
                eq_id = st.selectbox("Equipo", list(st.session_state.fleet.keys()), key="fail_roles_eq")
                categories = list(st.session_state.component_categories.keys())
                system_sel = st.selectbox("Sistema", categories, key="fail_roles_sys")
                comp_options = st.session_state.component_categories.get(system_sel, [])
                if comp_options:
                    comp_name = st.selectbox("Componente", comp_options, key="fail_roles_comp")
                else:
                    comp_name = st.text_input("Componente", key="fail_roles_comp_text")
                description = st.text_input("Descripción de la falla", key="fail_roles_desc")
                repair_hours = st.number_input("Horas de reparación", min_value=0.0, step=0.5, key="fail_roles_hours")
                logged = st.form_submit_button("Registrar falla")
                if logged:
                    fl.log_failure(eq_id, comp_name, description, repair_hours)
                    save_data("log falla")
                    st.success(f"Falla registrada para equipo {eq_id}")
        if fl.entries:
            st.write("Historial de fallas")
            eq_choices = list({entry[1] for entry in fl.entries})
            eq_choices.sort()
            eq_filter = st.selectbox("Filtrar por equipo", ["Todos"] + eq_choices, key="fail_hist_filter")
            rows = []
            for ts, eq_id, comp, desc, repair_h in fl.entries:
                if eq_filter != "Todos" and eq_id != eq_filter:
                    continue
                rows.append(
                    {"Fecha": ts.strftime("%Y-%m-%d %H:%M"), "Equipo": eq_id, "Componente": comp,
                     "Descripción": desc, "Horas reparación": repair_h}
                )
            if rows:
                st.table(rows)
                df_fails = pd.DataFrame(rows)
                csv = df_fails.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Descargar historial CSV",
                    data=csv,
                    file_name="historial_fallas.csv",
                    mime="text/csv",
                )
            else:
                st.info("No hay fallas para el filtro seleccionado.")
        else:
            st.info("No hay fallas registradas.")

    with tabs[8]:
        st.subheader("Métricas y confiabilidad")
        fl = st.session_state.failure_log
        if fl.entries:
            eq_ids = sorted({entry[1] for entry in fl.entries})
            st.markdown("**Métricas globales por equipo**")
            global_rows = []
            for eq in eq_ids:
                mtbf_eq = fl.calculate_mtbf(eq, None)
                mttr_eq = fl.calculate_mttr(eq, None)
                cnt = sum(1 for e in fl.entries if e[1] == eq)
                global_rows.append(
                    {"Equipo": eq, "Fallas": cnt, "MTBF": mtbf_eq if mtbf_eq is not None else 0,
                     "MTTR": mttr_eq if mttr_eq is not None else 0}
                )
            df_global = pd.DataFrame(global_rows)
            st.write("Número de fallas por equipo")
            st.bar_chart(df_global.set_index("Equipo")["Fallas"])
            st.write("MTBF y MTTR por equipo (horas)")
            st.line_chart(df_global.set_index("Equipo")[["MTBF", "MTTR"]])
            st.download_button(
                label="Descargar métricas globales CSV",
                data=df_global.to_csv(index=False).encode("utf-8"),
                file_name="metricas_globales.csv",
                mime="text/csv",
            )
            st.markdown("---")
            st.markdown("**Detalles por equipo y componente**")
            eq_sel = st.selectbox("Seleccione un equipo para ver detalles", eq_ids, key="metrics_eq")
            comp_names = sorted({entry[2] for entry in fl.entries if entry[1] == eq_sel})
            mtbf_vals, mttr_vals, fail_counts = [], [], []
            for comp in comp_names:
                mtbf = fl.calculate_mtbf(eq_sel, comp)
                mttr = fl.calculate_mttr(eq_sel, comp)
                count = sum(1 for e in fl.entries if e[1] == eq_sel and e[2] == comp)
                mtbf_vals.append(mtbf if mtbf is not None else 0)
                mttr_vals.append(mttr if mttr is not None else 0)
                fail_counts.append(count)
            data = {"Componente": comp_names, "Fallas": fail_counts, "MTBF": mtbf_vals, "MTTR": mttr_vals}
            df_detail = pd.DataFrame(data)
            if not df_detail.empty:
                st.write("Número de fallas por componente")
                st.bar_chart(df_detail.set_index("Componente")["Fallas"])
                st.write("MTBF y MTTR (horas)")
                st.line_chart(df_detail.set_index("Componente")[["MTBF", "MTTR"]])
                overall_mtbf = fl.calculate_mtbf(eq_sel, None)
                if overall_mtbf and overall_mtbf > 0:
                    import numpy as np
                    import matplotlib.pyplot as plt
                    st.write("Curva de confiabilidad (modelo exponencial)")
                    times = np.linspace(0, overall_mtbf * 3, 100)
                    rel = np.exp(-times / overall_mtbf)
                    fig, ax = plt.subplots()
                    ax.plot(times, rel)
                    ax.set_xlabel("Horas de operación")
                    ax.set_ylabel("Confiabilidad R(t)")
                    ax.set_title(f"Confiabilidad para {eq_sel} (MTBF = {overall_mtbf:.1f} h)")
                    st.pyplot(fig)
                else:
                    st.info("No se dispone de MTBF global para trazar la confiabilidad.")
                st.download_button(
                    label=f"Descargar métricas de {eq_sel}",
                    data=df_detail.to_csv(index=False).encode("utf-8"),
                    file_name=f"metricas_{eq_sel}.csv",
                    mime="text/csv",
                )
            else:
                st.info("El equipo seleccionado no tiene fallas registradas.")
        else:
            st.info("No se han registrado fallas; no hay métricas disponibles.")

    with tabs[9]:
        st.subheader("Reporte de disponibilidad de la flota")
        total, available, due_soon, in_maintenance = fleet_summary()
        fail_eq = {
            req["equipment_id"]
            for req in st.session_state.work_requests
            if req["status"] == "pendiente" and req["classification"] == "alta"
        }
        failure = len(fail_eq)
        counts = {
            "Disponible": available,
            "En mantenimiento": in_maintenance,
            "Próx. mantenimiento": due_soon,
            "Falla": failure,
        }
        df_counts = pd.DataFrame.from_dict(counts, orient="index", columns=["Cantidad"])
        st.bar_chart(df_counts)

# ---------------------------------------------------------------------------

def main() -> None:
    st.set_page_config(
        page_title="Gestión de Mantenimiento (Roles)",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    init_state()

    st.markdown(
        """
        <style>
        .stApp { background-color: #ffffff; color: #333333; }
        h1, h2, h3, h4, h5, h6 { color: #c62828; }
        .stButton>button { background-color: #c62828; color: #ffffff; }
        .css-1v0mbdj h1, .css-1v0mbdj h2, .css-1v0mbdj h3 { color: #c62828; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if os.path.exists(LOGO_PATH):
        try:
            st.sidebar.image(LOGO_PATH, use_column_width=True)
        except Exception:
            pass

    if not st.session_state.get("logged_in", False):
        st.sidebar.title("Inicio de sesión")
        with st.sidebar.form("login_form"):
            username = st.text_input("Usuario", key="login_user")
            password = st.text_input("Contraseña", type="password", key="login_pass")
            login_submitted = st.form_submit_button("Entrar")
        if login_submitted:
            user_entry = USERS.get(username)
            if user_entry and user_entry["password"] == password:
                st.session_state.logged_in = True
                st.session_state.user = username
                st.session_state.role = user_entry["role"]
                st.sidebar.success(f"Bienvenido, {username}")
                st.experimental_rerun()
            else:
                st.sidebar.error("Credenciales incorrectas")
        st.title("Gestión de Mantenimiento de Flota – Inicio de sesión")
        st.write("Ingrese sus credenciales en la barra lateral para acceder a la aplicación.")
        return
    else:
        current_user = st.session_state.user
        current_role = st.session_state.role
        st.sidebar.write(f"Usuario: {current_user} ({current_role})")
        with st.sidebar.expander("Cambiar contraseña"):
            with st.form("change_pass_form"):
                old_pass = st.text_input("Contraseña actual", type="password")
                new_pass = st.text_input("Nueva contraseña", type="password")
                confirm_pass = st.text_input("Confirmar nueva contraseña", type="password")
                change_submitted = st.form_submit_button("Actualizar contraseña")
            if change_submitted:
                user_entry = USERS.get(current_user)
                if user_entry and user_entry["password"] == old_pass:
                    if not new_pass:
                        st.warning("La nueva contraseña no puede estar vacía")
                    elif new_pass != confirm_pass:
                        st.warning("La nueva contraseña y su confirmación no coinciden")
                    else:
                        USERS[current_user]["password"] = new_pass
                        save_data("cambio contraseña")
                        st.success("Contraseña actualizada correctamente")
                else:
                    st.error("La contraseña actual es incorrecta")

        if st.sidebar.button("Cerrar sesión"):
            for key in ["logged_in", "user", "role"]:
                st.session_state.pop(key, None)
            st.experimental_rerun()

        st.title("Gestión de Mantenimiento de Flota – Roles")
        st.write("Seleccione las pestañas para acceder a las funciones disponibles. Tanto mantenimiento como operaciones comparten un resumen de la flota.")

        role = st.session_state.role
        if role == "Mantenimiento":
            maintenance_view()
        else:
            operations_view()

if __name__ == "__main__":
    main()
