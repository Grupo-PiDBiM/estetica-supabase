# ==========================================================
# Estética | Turnos tipo Calendly (con Supabase)
# Landing + Reserva paso a paso + Admin (Agenda, Servicios, Clientes, Historial)
# Persistencia 100% en Supabase (Postgres)
# - Horarios en selectbox (mobile friendly) + bloquea horarios pasados del día actual
# - Grupos exclusivos (Piernas/Brazos/Rostro) + zonas extra
# - Editor masivo de turnos, catálogo editable, clientes editable
# - Finalizar turno y archivar historial por cliente + historial global
# - En "Turno pendiente" y "Cliente existente": "Nombre – email"
# - Estilos responsive para celular
#
# Requisitos:
# - st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"]
# - pip install streamlit supabase pandas
# ==========================================================
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, time, date
import uuid
import re
from typing import List, Optional, Dict, Any

# =========================
# CONFIG GENERAL
# =========================
st.set_page_config(page_title="Turnos Estética", page_icon="💆‍♀️", layout="wide")
APP_TITLE = "💆‍♀️ Turnos Estética"

# Admin (podés mover a st.secrets si querés)
ADMIN_USER = st.secrets.get("ADMIN_USER", "admin")
ADMIN_PASS = st.secrets.get("ADMIN_PASS", "admin")

# Parámetros de turnos
SLOT_STEP_MIN = 10
BUFFER_MIN_DEFAULT = 5

# Disponibilidad semanal (1=Lun ... 7=Dom)
# ➜ Ahora: todos los días menos domingo (6 = sábado habilitado)
DEFAULT_DISPONIBILIDAD_CODE = {
    1: [("09:00", "13:00"), ("14:00", "17:00")],  # Lunes
    2: [("09:00", "17:00")],                      # Martes
    3: [("09:00", "17:00")],                      # Miércoles
    4: [("09:00", "17:00")],                      # Jueves
    5: [("09:00", "15:00")],                      # Viernes
    6: [("09:00", "15:00")],                      # Sábado
    # 7: sin turnos (Domingo)
}

# =========================
# SUPABASE CLIENT
# =========================
from supabase import create_client, Client

@st.cache_resource(show_spinner=False)
def get_supabase() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = get_supabase()

# =========================
# UTILS
# =========================
def to_time(hhmm: str) -> Optional[time]:
    s = str(hhmm).strip()
    if not s or ":" not in s:
        return None
    try:
        hh, mm = s.split(":")[:2]
        return time(int(hh), int(mm))
    except Exception:
        return None

def overlaps(start1, end1, start2, end2):
    return (start1 < end2) and (start2 < end1)

def humanize_list(items: Optional[List[str]]):
    if not items:
        return ""
    return ", ".join([str(x) for x in items if str(x).strip() != ""])

def slugify(text: str) -> str:
    text = str(text or "").strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "-", text)
    return text[:60] if text else "cliente"

def format_ars(n: int | float) -> str:
    s = f"{n:,.0f}"
    return "AR$ " + s.replace(",", ".")

def norm_phone(s: str) -> str:
    return re.sub(r"\D+", "", str(s or ""))

# =========================
# DATA ACCESS (Supabase)
# =========================
# Tablas esperadas:
# - servicios: id(uuid), tipo, zona, duracion_min, precio, unique(tipo,zona)
# - clientes: cliente_id(uuid), nombre, whatsapp, email, notas
# - turnos: turno_id, cliente_id(uuid), fecha, inicio, fin, tipo, zonas,
#           duracion_total, estado, notas, recordatorio_enviado
# - historial: id, cliente_id(uuid), nombre, fecha, evento, detalles

# --- SERVICIOS ---
def db_get_servicios() -> pd.DataFrame:
    res = supabase.table("servicios").select("*").execute()
    df = pd.DataFrame(res.data or [])
    if df.empty:
        return df
    df.columns = [c.lower() for c in df.columns]
    for c in ["duracion_min", "precio"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df

def db_save_servicios(df: pd.DataFrame):
    records = df.fillna("").to_dict(orient="records")
    for r in records:
        r["duracion_min"] = int(pd.to_numeric(r.get("duracion_min", 0)))
        r["precio"] = int(pd.to_numeric(r.get("precio", 0)))
        supabase.table("servicios").upsert(r, on_conflict="tipo,zona").execute()

# --- CLIENTES ---
def db_get_clientes() -> pd.DataFrame:
    res = supabase.table("clientes").select("*").execute()
    df = pd.DataFrame(res.data or [])
    if df.empty:
        return df
    df.columns = [c.lower() for c in df.columns]
    for c in ["cliente_id", "whatsapp"]:
        if c in df.columns:
            df[c] = df[c].astype(str)
    return df

def db_upsert_cliente(cliente_id: str, nombre: str, whatsapp: str, email: str, notas: str = "") -> str:
    """
    Opción 2 — cliente_id es un UUID independiente del teléfono.
    Si cliente_id viene vacío → genera uno nuevo.
    Si cliente_id existe → actualiza ese registro.
    """
    cid = (cliente_id or "").strip()
    if not cid:
        cid = str(uuid.uuid4())

    rec = {
        "cliente_id": cid,
        "nombre": (nombre or "").strip(),
        "whatsapp": norm_phone(whatsapp),
        "email": (email or "").strip(),
        "notas": (notas or "").strip(),
    }
    supabase.table("clientes").upsert(rec, on_conflict="cliente_id").execute()
    return cid

# --- TURNOS ---
def db_get_turnos() -> pd.DataFrame:
    res = supabase.table("turnos").select("*").execute()
    df = pd.DataFrame(res.data or [])
    if df.empty:
        return df
    df.columns = [c.lower() for c in df.columns]
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date
    return df

def db_insert_turno(row: Dict[str, Any]):
    supabase.table("turnos").insert(row).execute()

def db_update_turnos(df_out: pd.DataFrame):
    # Actualiza fila por fila por PK turno_id
    for _, r in df_out.iterrows():
        rid = str(r.get("turno_id", ""))
        data = {
            k: (None if (isinstance(v, float) and pd.isna(v)) else v)
            for k, v in r.to_dict().items()
            if k != "turno_id"
        }
        supabase.table("turnos").update(data).eq("turno_id", rid).execute()

# --- HISTORIAL ---
def db_add_historial(cliente_id: str, nombre: str, evento: str, detalles: str):
    supabase.table("historial").insert({
        "id": str(uuid.uuid4()),
        "cliente_id": cliente_id,
        "nombre": nombre,
        "fecha": datetime.utcnow().isoformat(),
        "evento": evento,
        "detalles": detalles,
    }).execute()

def get_cliente_display_row(row: dict | pd.Series) -> str:
    nombre = str(row.get("nombre", "") or "").strip()
    email = str(row.get("email", "") or "").strip()
    cid = str(row.get("cliente_id", "") or "").strip()
    if nombre and email:
        return f"{nombre} – {email}"
    if nombre:
        return nombre
    return cid or "Sin nombre"

# =========================
# LÓGICA NEGOCIO
# =========================
def calc_duracion(servicios_df: pd.DataFrame, tipo: str, zonas: List[str]) -> int:
    if servicios_df.empty:
        return 0
    sel = servicios_df[(servicios_df["tipo"] == tipo) & (servicios_df["zona"].isin(zonas))]
    return int(sel["duracion_min"].sum()) if not sel.empty else 0

def calc_precio(servicios_df: pd.DataFrame, tipo: str, zonas: List[str]) -> int:
    if servicios_df.empty:
        return 0
    sel = servicios_df[(servicios_df["tipo"] == tipo) & (servicios_df["zona"].isin(zonas))]
    return int(sel["precio"].sum()) if not sel.empty else 0

def generar_slots(date_obj: date, dur_min: int, turnos_df: pd.DataFrame, slot_step_min: int = SLOT_STEP_MIN):
    """
    Genera slots disponibles para una fecha.
    ➜ Cambio: cualquier turno que NO esté en 'Cancelado' bloquea su rango horario.
    """
    if dur_min <= 0:
        return []
    weekday = date_obj.isoweekday()
    tramos = DEFAULT_DISPONIBILIDAD_CODE.get(weekday, [])
    if not tramos:
        return []

    activos = pd.DataFrame()
    if not turnos_df.empty:
        # Antes excluíamos Cancelado, No-show y Realizado.
        # Ahora: solo los Cancelado liberan el horario.
        activos = turnos_df[
            (turnos_df["fecha"] == date_obj) &
            (~turnos_df["estado"].isin(["Cancelado"]))
        ].copy()

    result = []
    step = timedelta(minutes=slot_step_min)
    dur = timedelta(minutes=dur_min)
    buff = timedelta(minutes=BUFFER_MIN_DEFAULT)

    for (ini, fin) in tramos:
        ti, tf = to_time(ini), to_time(fin)
        if not ti or not tf:
            continue
        start_dt = datetime.combine(date_obj, ti)
        end_window = datetime.combine(date_obj, tf)
        current = start_dt
        while current + dur <= end_window:
            c_start, c_end = current, current + dur
            ok = True
            if not activos.empty:
                for _, t in activos.iterrows():
                    try:
                        t_start = datetime.combine(
                            date_obj,
                            datetime.strptime(str(t["inicio"]), "%H:%M").time()
                        )
                        t_end = datetime.combine(
                            date_obj,
                            datetime.strptime(str(t["fin"]), "%H:%M").time()
                        )
                    except Exception:
                        continue
                    if overlaps(c_start - buff, c_end + buff, t_start, t_end):
                        ok = False
                        break
            if ok:
                result.append(c_start)
            current += step

    # únicos y ordenados
    seen: Dict[datetime, datetime] = {}
    return [seen.setdefault(x, x) for x in result if x not in seen]

def filter_future_slots(date_obj: date, slots: List[datetime]) -> List[datetime]:
    if not slots:
        return []
    now = datetime.now()
    if date_obj == now.date():
        return [s for s in slots if s > now]
    return slots

# =========================
# ESTADO INICIAL
# =========================
if "vista" not in st.session_state:
    st.session_state["vista"] = "home"

_defaults_booking_state = {
    "step": "pick_service",  # pick_service -> pick_date -> pick_time -> client_details -> confirm
    "service_tipo": None,
    "service_zonas": None,
    "duracion": 0,
    "precio_total": 0,
    "fecha": None,
    "slot_dt": None,
    "nombre": "",
    "whatsapp": "",
    "email": "",
    "notas": "",
}
if "booking" not in st.session_state:
    st.session_state["booking"] = _defaults_booking_state.copy()

# =========================
# ESTILOS (CSS simple)
# =========================
st.markdown(
    """
<style>
:root {
  --card-bg:#fff;
  --card-br:14px;
  --card-bd:1px solid #ececec;
  --soft-shadow:0 2px 10px rgba(0,0,0,0.06);
}
.card { background:var(--card-bg); border:var(--card-bd); border-radius:var(--card-br); padding:16px; box-shadow:var(--soft-shadow); }
.step-title { font-weight:700; font-size:20px; margin:6px 0 14px; }
.touch-btn button, .touch-full button { padding:10px 14px !important; border-radius:10px !important; }
.badge { display:inline-block; padding:2px 8px; border-radius:10px; background:#EEF2FF; color:#344; font-size:12px; margin-right:6px; }
.small { font-size:13px; color:#666; }
hr { border:none; border-top:1px solid #eee; margin:8px 0 16px; }
.confirm-box { background:#F6FFED; border:1px solid #B7EB8F; border-radius:12px; padding:16px; }
@media (max-width:768px){
  .step-title{font-size:18px;}
  .card{padding:12px;}
  .touch-full button{ width:100% !important; }
}
</style>
""",
    unsafe_allow_html=True,
)

# =========================
# HEADER
# =========================
st.title(APP_TITLE)

# =========================
# Helpers
# =========================
def go_home():
    st.session_state["vista"] = "home"
    st.rerun()

# =========================
# HOME
# =========================
if st.session_state["vista"] == "home":
    left, right = st.columns([3, 1])
    with left:
        st.markdown("## Bienvenida 👋")
        st.write("Elegí una opción para continuar.")
        if st.button("🗓️ Reservar turno", type="primary", use_container_width=True):
            st.session_state["vista"] = "reserva"
            st.session_state["booking"] = _defaults_booking_state.copy()
            st.rerun()
    with right:
        st.markdown("#### Acceso")
        if st.button("🔑 Panel del administrador", use_container_width=True):
            st.session_state["vista"] = "login_admin"
            st.rerun()
    st.stop()

# =========================
# LOGIN ADMIN
# =========================
if st.session_state["vista"] == "login_admin":
    st.markdown("### 🔐 Ingresar al panel")
    colA, colB = st.columns(2)
    user = colA.text_input("Usuario")
    pwd = colB.text_input("Contraseña", type="password")
    c1, c2 = st.columns([1, 3])
    if c1.button("Ingresar", type="primary"):
        if user == ADMIN_USER and pwd == ADMIN_PASS:
            st.session_state["vista"] = "admin"
            st.rerun()
        else:
            st.error("Usuario o contraseña incorrectos.")
    if c2.button("⬅ Volver al inicio"):
        go_home()
    st.stop()

# =========================
# RESERVA — TIPO CALENDLY
# =========================
if st.session_state["vista"] == "reserva":
    servicios_df = db_get_servicios()
    turnos_df = db_get_turnos()
    _clientes_df = db_get_clientes()  # reservado por si luego se usa

    if st.button("⬅ Volver al inicio"):
        go_home()

    st.markdown("### Reservá tu turno en 3 pasos")
    booking = st.session_state["booking"]

    # STEP 1 — Elegir Servicio
    if booking["step"] == "pick_service":
        st.markdown('<div class="step-title">1) Elegí tu servicio</div>', unsafe_allow_html=True)

        if servicios_df.empty:
            st.warning("No hay servicios cargados. Volvé más tarde.")
            st.stop()

        tipos_raw = [
            t for t in servicios_df["tipo"].dropna().astype(str).unique().tolist()
            if t.strip() != ""
        ]
        prefer = ["Descartable", "Láser"]
        tipos = [t for t in prefer if t in tipos_raw] + [t for t in tipos_raw if t not in prefer]

        tipo_sel = st.selectbox("Tipo", tipos, index=0, key="tipo_sel")
        zonas_tipo = servicios_df[
            (servicios_df["tipo"] == tipo_sel) &
            (servicios_df["zona"].str.strip() != "")
        ]["zona"].unique().tolist()

        GROUP_RULES = {
            "Piernas": ["Medias piernas", "Piernas completas"],
            "Brazos":  ["Brazos", "Medio brazo"],
            "Rostro":  ["Rostro completo", "Cara"],
        }

        with st.container():
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.markdown("##### Zonas")
            seleccion_grupos = []
            for grupo, miembros in GROUP_RULES.items():
                presentes = [m for m in miembros if m in zonas_tipo]
                if len(presentes) >= 2:
                    choice = st.radio(
                        f"{grupo}",
                        ["Ninguna"] + presentes,
                        index=0,
                        horizontal=True,
                        key=f"radio_{tipo_sel}_{grupo}",
                    )
                    if choice != "Ninguna":
                        seleccion_grupos.append(choice)

            usados_en_grupos = {m for ml in GROUP_RULES.values() for m in ml}
            zonas_sueltas = [z for z in zonas_tipo if z not in usados_en_grupos]
            zonas_extra = (
                st.multiselect(
                    "Otras zonas (podés elegir varias)",
                    zonas_sueltas,
                    key=f"otras_{tipo_sel}",
                )
                if zonas_sueltas
                else []
            )
            st.markdown("</div>", unsafe_allow_html=True)

        zonas_final = list(dict.fromkeys(seleccion_grupos + zonas_extra))
        dur_preview = calc_duracion(servicios_df, tipo_sel, zonas_final) if zonas_final else 0
        precio_preview = calc_precio(servicios_df, tipo_sel, zonas_final) if zonas_final else 0

        c1, c2, c3 = st.columns([1, 1, 2])
        c1.metric("Duración total", f"{dur_preview} min")
        c2.metric("Precio estimado", format_ars(precio_preview))
        c3.caption("La duración y el precio se calculan sumando todas las zonas elegidas.")

        if st.button("Continuar ➡️", type="primary", use_container_width=True):
            if not zonas_final:
                st.warning("Elegí al menos una zona.")
            else:
                booking["service_tipo"] = tipo_sel
                booking["service_zonas"] = zonas_final
                booking["duracion"] = dur_preview
                booking["precio_total"] = precio_preview
                booking["step"] = "pick_date"
                st.session_state["booking"] = booking
                st.rerun()

    # STEP 2 — Fecha
    if booking["step"] == "pick_date":
        st.markdown('<div class="step-title">2) Elegí la fecha</div>', unsafe_allow_html=True)
        st.caption(
            f"Servicio: **{booking['service_tipo']}** — "
            f"Zonas: **{humanize_list(booking['service_zonas'] or [])}** — "
            f"⏱ {booking['duracion']} min — {format_ars(booking['precio_total'])}"
        )

        c1, c2 = st.columns([1, 3])
        with c1:
            fecha = st.date_input(
                "Fecha",
                min_value=date.today(),
                value=booking.get("fecha") or date.today(),
            )
            if st.button("⬅ Cambiar zonas"):
                st.session_state["booking"] = _defaults_booking_state.copy()
                st.session_state["booking"]["step"] = "pick_service"
                st.rerun()
        with c2:
            st.info("Luego vas a elegir el horario disponible.")

        if st.button("Siguiente ➡️", type="primary"):
            if not fecha:
                st.warning("Elegí una fecha.")
            else:
                booking["fecha"] = fecha
                booking["step"] = "pick_time"
                st.session_state["booking"] = booking
                st.rerun()

    # STEP 3 — Horario
    if booking["step"] == "pick_time":
        st.markdown('<div class="step-title">3) Elegí el horario</div>', unsafe_allow_html=True)
        st.caption(
            f"{booking['fecha']} — {booking['service_tipo']} / "
            f"{humanize_list(booking['service_zonas'] or [])} — "
            f"⏱ {booking['duracion']} min — {format_ars(booking['precio_total'])}"
        )

        if not booking.get("fecha"):
            st.warning("Elegí una fecha.")
        else:
            turnos_df = db_get_turnos()  # refresco
            slots_all = generar_slots(booking["fecha"], booking["duracion"], turnos_df, SLOT_STEP_MIN)
            slots = filter_future_slots(booking["fecha"], slots_all)
            if not slots:
                st.error("No hay horarios disponibles para esa fecha.")
            else:
                opciones = [s.strftime("%H:%M") for s in slots]
                current_label = booking["slot_dt"].strftime("%H:%M") if booking.get("slot_dt") else None
                label_idx = opciones.index(current_label) if current_label in opciones else 0
                sel_label = st.selectbox(
                    "Horario disponible",
                    opciones,
                    index=label_idx,
                    key="select_hora",
                )
                sel_dt = [s for s in slots if s.strftime("%H:%M") == sel_label][0]
                if (not booking.get("slot_dt")) or (booking["slot_dt"] != sel_dt):
                    booking["slot_dt"] = sel_dt
                    st.session_state["booking"] = booking

        c1, c2 = st.columns(2)
        if c1.button("⬅ Volver a fecha"):
            booking["step"] = "pick_date"
            st.session_state["booking"] = booking
            st.rerun()
        disabled_next = booking.get("slot_dt") is None
        if c2.button("Siguiente ➡️", type="primary", disabled=disabled_next):
            booking["step"] = "client_details"
            st.session_state["booking"] = booking
            st.rerun()

    # STEP 4 — Datos del cliente
    if booking["step"] == "client_details":
        st.markdown('<div class="step-title">4) Tus datos</div>', unsafe_allow_html=True)
        st.caption(
            f"{booking['fecha']} — "
            f"{booking['slot_dt'].strftime('%H:%M') if booking.get('slot_dt') else ''} — "
            f"{booking['service_tipo']} / {humanize_list(booking['service_zonas'] or [])}"
        )

        with st.form("client_form"):
            c1, c2 = st.columns(2)
            nombre = c1.text_input("Nombre y apellido", value=booking.get("nombre", ""))
            whatsapp = c2.text_input("WhatsApp (+549...)", value=booking.get("whatsapp", ""))
            email = st.text_input("Email (opcional)", value=booking.get("email", ""))
            notas = st.text_area("Notas (opcional)", value=booking.get("notas", ""))
            submitted = st.form_submit_button("Confirmar turno ✅")
        if submitted:
            if not nombre.strip() or not whatsapp.strip() or not booking.get("slot_dt"):
                st.warning("Completá nombre, WhatsApp y elegí un horario.")
            else:
                # Opción 2: siempre crea un cliente nuevo con UUID
                cid = db_upsert_cliente("", nombre, whatsapp, email)

                inicio_str = booking["slot_dt"].strftime("%H:%M")
                fin_str = (booking["slot_dt"] + timedelta(minutes=booking["duracion"])).strftime("%H:%M")
                turno_id = str(uuid.uuid4())
                zonas_str = humanize_list(booking["service_zonas"] or [])
                new_row = {
                    "turno_id": turno_id,
                    "cliente_id": cid,
                    "fecha": booking["fecha"].strftime("%Y-%m-%d"),
                    "inicio": inicio_str,
                    "fin": fin_str,
                    "tipo": booking["service_tipo"],
                    "zonas": zonas_str,
                    "duracion_total": int(booking["duracion"]),
                    "estado": "Confirmado",
                    "notas": (notas or "").strip(),
                    "recordatorio_enviado": False,
                }
                db_insert_turno(new_row)

                booking["nombre"] = nombre.strip()
                booking["whatsapp"] = whatsapp.strip()
                booking["email"] = email.strip()
                booking["notas"] = (notas or "").strip()
                booking["step"] = "confirm"
                st.session_state["booking"] = booking
                st.rerun()

        if st.button("⬅ Volver a horario"):
            booking["step"] = "pick_time"
            st.session_state["booking"] = booking
            st.rerun()

    # STEP 5 — Confirmación
    if booking["step"] == "confirm":
        st.success("¡Listo! Tu turno fue confirmado ✅")
        st.markdown(
            """
        <div class="confirm-box">
        <h4>¡Gracias por reservar!</h4>
        <p>Estos son los detalles de tu turno:</p>
        </div>
        """,
            unsafe_allow_html=True,
        )
        det1, det2 = st.columns(2)
        with det1:
            st.write(f"**Servicio:** {booking['service_tipo']}")
            st.write(f"**Zonas:** {humanize_list(booking['service_zonas'] or [])}")
            st.write(f"**Duración:** {booking['duracion']} min")
            st.write(f"**Precio estimado:** {format_ars(booking['precio_total'])}")
        with det2:
            st.write(f"**Fecha:** {booking['fecha']}")
            st.write(f"**Horario:** {booking['slot_dt'].strftime('%H:%M') if booking.get('slot_dt') else ''}")
            st.write(f"**Nombre:** {booking['nombre']}")
            st.write(f"**WhatsApp:** {booking['whatsapp']}")
            if booking.get("email"):
                st.write(f"**Email:** {booking['email']}")
        st.info("Te vamos a recordar tu turno el día anterior 💬")

        colx, coly = st.columns(2)
        if colx.button("📅 Reservar otro turno"):
            st.session_state["booking"] = _defaults_booking_state.copy()
            st.rerun()
        if coly.button("🏠 Volver al inicio"):
            go_home()

# =========================
# PANEL ADMIN
# =========================
if st.session_state["vista"] == "admin":
    top1, top2 = st.columns([1, 3])
    if top1.button("⬅ Volver al inicio"):
        go_home()
    st.success("Ingreso correcto ✅")

    tab_turnos, tab_servicios, tab_clientes, tab_historial = st.tabs(
        ["📆 Turnos", "🧾 Servicios", "👤 Clientes", "📓 Historial"]
    )

    # -------- 📆 TURNOS
    with tab_turnos:
        turnos_df = db_get_turnos()
        clientes_df = db_get_clientes()

        st.markdown("#### Base de turnos (con filtros)")
        c1, c2, c3 = st.columns([1, 1, 2])
        desde = c1.date_input("Desde", value=date.today())
        hasta = c2.date_input("Hasta", value=date.today() + timedelta(days=14))
        filtro_estado = c3.multiselect(
            "Estado",
            options=["Confirmado", "Reprogramado", "Cancelado", "No-show", "Realizado"],
            default=["Confirmado", "Reprogramado"],
        )

        df_agenda = turnos_df.copy()
        if not df_agenda.empty:
            df_agenda = df_agenda[(df_agenda["fecha"] >= desde) & (df_agenda["fecha"] <= hasta)]
            if filtro_estado:
                df_agenda = df_agenda[df_agenda["estado"].isin(filtro_estado)]
        if df_agenda.empty:
            st.info("Sin turnos en el rango / estado seleccionado.")
        else:
            if not clientes_df.empty:
                nombre_map = clientes_df.set_index("cliente_id").apply(
                    lambda r: get_cliente_display_row(r),
                    axis=1,
                ).to_dict()
                df_agenda["cliente"] = df_agenda["cliente_id"].map(nombre_map).fillna(df_agenda["cliente_id"])
            cols = ["fecha", "inicio", "fin", "cliente", "tipo", "zonas", "estado", "notas", "turno_id"]
            show = [c for c in cols if c in df_agenda.columns]
            st.dataframe(
                df_agenda[show].sort_values(by=["fecha", "inicio"]),
                use_container_width=True,
            )

        st.divider()
        st.markdown("### 🛠️ Editar turnos (toda la base)")
        base_turnos = turnos_df.copy()
        if base_turnos.empty:
            st.info("No hay turnos activos.")
        else:
            estado_options = ["Confirmado", "Reprogramado", "Cancelado", "No-show", "Realizado"]
            base_edit = base_turnos.copy()
            base_edit["fecha"] = base_edit["fecha"].astype(str)

            # Columna Cliente (Nombre – email) para que sea más legible
            if not clientes_df.empty:
                cliente_map = clientes_df.set_index("cliente_id").apply(
                    lambda r: get_cliente_display_row(r),
                    axis=1,
                ).to_dict()
                base_edit["cliente"] = base_edit["cliente_id"].map(cliente_map).fillna(base_edit["cliente_id"])
            else:
                base_edit["cliente"] = base_edit["cliente_id"]

            edit_turnos = st.data_editor(
                base_edit[
                    [
                        "turno_id",
                        "cliente_id",
                        "cliente",        # visible y solo lectura
                        "fecha",
                        "inicio",
                        "fin",
                        "tipo",
                        "zonas",
                        "duracion_total",
                        "estado",
                        "notas",
                    ]
                ],
                num_rows="dynamic",
                use_container_width=True,
                key="edit_turnos_all",
                column_config={
                    "cliente": st.column_config.TextColumn(disabled=True, help="Nombre – email"),
                    "estado": st.column_config.SelectboxColumn(options=estado_options),
                    "fecha": st.column_config.TextColumn(help="YYYY-MM-DD"),
                    "inicio": st.column_config.TextColumn(help="HH:MM"),
                    "fin": st.column_config.TextColumn(help="HH:MM"),
                    "notas": st.column_config.TextColumn(width="large"),
                },
            )
            if st.button("💾 Guardar cambios de turnos"):
                out = edit_turnos.copy()
                # Eliminamos la columna Cliente (es solo visual)
                if "cliente" in out.columns:
                    out = out.drop(columns=["cliente"])
                out["fecha"] = pd.to_datetime(out["fecha"], errors="coerce").dt.date.astype(str)
                if "recordatorio_enviado" not in out.columns:
                    out["recordatorio_enviado"] = False
                db_update_turnos(out)
                st.success("Cambios guardados.")
                st.rerun()

        st.divider()
        st.markdown("### ✅ Finalizar turno y archivar")

        # Solo muestra turnos que YA fueron marcados como "Realizado"
        pendientes = turnos_df[turnos_df["estado"] == "Realizado"]
        if pendientes.empty:
            st.info("No hay turnos con estado 'Realizado' para archivar.")
        else:
            def fmt_turno(tid: str) -> str:
                row = pendientes[pendientes["turno_id"] == tid].iloc[0]
                etiqueta_cliente = str(row["cliente_id"])
                if not clientes_df.empty and (row["cliente_id"] in clientes_df["cliente_id"].values):
                    cli_row = clientes_df[clientes_df["cliente_id"] == row["cliente_id"]].iloc[0]
                    etiqueta_cliente = get_cliente_display_row(cli_row)
                return f"{etiqueta_cliente} | {row['fecha']} {row['inicio']} | {row['tipo']} - {row['zonas']}"

            sel_turno_id = st.selectbox(
                "Turno marcado como 'Realizado'",
                pendientes["turno_id"].tolist(),
                format_func=fmt_turno,
            )

            colA, colB = st.columns([2, 2])
            is_new = colB.checkbox("Cliente nuevo")

            if clientes_df.empty and not is_new:
                st.warning("No hay clientes cargados. Marcá 'Cliente nuevo'.")
                nuevo_nombre = nuevo_whats = nuevo_email = ""
            else:
                if is_new:
                    n1, n2 = st.columns(2)
                    nuevo_nombre = n1.text_input("Nombre y apellido *")
                    nuevo_whats = n2.text_input("WhatsApp (+549...) *")
                    nuevo_email = st.text_input("Email")
                else:
                    cliente_ids = clientes_df["cliente_id"].astype(str).tolist()

                    def fmt_cliente(cid: str) -> str:
                        row = clientes_df[clientes_df["cliente_id"] == cid].iloc[0]
                        return get_cliente_display_row(row)

                    sel_cliente_id = st.selectbox("Cliente existente", cliente_ids, format_func=fmt_cliente)
                    row_sel = clientes_df[clientes_df["cliente_id"] == sel_cliente_id].iloc[0]
                    nuevo_nombre = str(row_sel.get("nombre", "") or "")
                    nuevo_whats = str(row_sel.get("whatsapp", "") or "")
                    nuevo_email = str(row_sel.get("email", "") or "")

            notas_adic = st.text_area("Notas adicionales para el archivo (opcional)")

            if st.button("Finalizar y archivar", type="primary"):
                # Traemos el turno seleccionado
                turnos = db_get_turnos()
                ix = turnos.index[turnos["turno_id"] == sel_turno_id].tolist()
                if not ix:
                    st.error("No se encontró el turno.")
                else:
                    row_turno = turnos.loc[ix[0]]

                    if is_new:
                        if not nuevo_nombre.strip() or not nuevo_whats.strip():
                            st.error("Completá nombre y WhatsApp para crear cliente nuevo.")
                            st.stop()
                        cid_final = db_upsert_cliente("", nuevo_nombre.strip(), nuevo_whats.strip(), nuevo_email.strip())
                    else:
                        cid_final = str(row_turno["cliente_id"])

                    notas_previas = str(row_turno.get("notas", "") or "")
                    if notas_adic.strip():
                        notas_final = (notas_previas + " | " if notas_previas else "") + notas_adic.strip()
                    else:
                        notas_final = notas_previvas = notas_previas

                    # Actualizamos turno con cliente_id definitivo y notas
                    upd = {
                        "cliente_id": cid_final,
                        "notas": notas_final,
                    }
                    supabase.table("turnos").update(upd).eq("turno_id", sel_turno_id).execute()

                    # Historial global
                    nombre_para_guardar = nuevo_nombre.strip()
                    if not nombre_para_guardar and not clientes_df.empty and (
                        cid_final in clientes_df["cliente_id"].values
                    ):
                        nombre_para_guardar = (
                            clientes_df[clientes_df["cliente_id"] == cid_final]
                            .iloc[0]
                            .get("nombre", "")
                        )

                    detalles = (
                        f"{row_turno.get('tipo', '')} | {row_turno.get('zonas', '')} | "
                        f"{row_turno.get('fecha', '')} {row_turno.get('inicio', '')}-"
                        f"{row_turno.get('fin', '')}"
                    )
                    db_add_historial(cid_final, nombre_para_guardar, "Turno finalizado", detalles)

                    st.success("Turno archivado en historial ✅")
                    st.rerun()

    # -------- 🧾 SERVICIOS
    with tab_servicios:
        servicios_df = db_get_servicios()
        st.markdown("#### Duraciones y costos")
        st.caption("Podés editar los valores directamente y guardar.")
        if servicios_df.empty:
            st.info("No hay servicios cargados. Creá filas nuevas y guardá.")
            servicios_df = pd.DataFrame([], columns=["tipo", "zona", "duracion_min", "precio"])
        edit_serv_tab = st.data_editor(
            servicios_df[["tipo", "zona", "duracion_min", "precio"]],
            num_rows="dynamic",
            use_container_width=True,
            key="edit_servicios_tab",
        )
        if st.button("💾 Guardar (servicios)", key="save_serv_tab"):
            db_save_servicios(edit_serv_tab)
            st.success("Servicios guardados.")
            st.rerun()

    # -------- 👤 CLIENTES
    with tab_clientes:
        clientes_df = db_get_clientes()
        st.markdown("#### Base de clientes")
        st.caption("Campos: cliente_id (UUID), nombre, whatsapp, email, notas")
        edit_cli = st.data_editor(
            clientes_df,
            num_rows="dynamic",
            use_container_width=True,
            key="edit_clientes",
        )
        if st.button("💾 Guardar clientes"):
            for _, r in edit_cli.fillna("").iterrows():
                db_upsert_cliente(
                    str(r.get("cliente_id", "")),
                    str(r.get("nombre", "")),
                    str(r.get("whatsapp", "")),
                    str(r.get("email", "")),
                    str(r.get("notas", "")),
                )
            st.success("Clientes guardados.")
            st.rerun()

    # -------- 📓 HISTORIAL
    with tab_historial:
        st.markdown("#### Historial por cliente")
        clientes_df = db_get_clientes()
        if clientes_df.empty:
            st.info("Aún no hay clientes cargados.")
        else:
            def fmt_cliente(cid: str) -> str:
                row = clientes_df[clientes_df["cliente_id"] == cid].iloc[0]
                return get_cliente_display_row(row)

            cliente_ids = clientes_df["cliente_id"].astype(str).tolist()
            sel_cliente_id = st.selectbox("Elegí un cliente", cliente_ids, format_func=fmt_cliente)

            row_cli = clientes_df[clientes_df["cliente_id"] == sel_cliente_id].iloc[0]
            c1, c2, c3 = st.columns(3)
            c1.metric("Nombre", str(row_cli.get("nombre", "") or "-"))
            c2.metric("WhatsApp", str(row_cli.get("whatsapp", "") or "-"))
            c3.metric("Email", str(row_cli.get("email", "") or "-"))

            res = (
                supabase.table("historial")
                .select("*")
                .eq("cliente_id", sel_cliente_id)
                .order("fecha", desc=True)
                .execute()
            )
            df_cli_hist = pd.DataFrame(res.data or [])
            if df_cli_hist.empty:
                st.info("Este cliente aún no tiene historial cargado.")
            else:
                df_cli_hist = df_cli_hist.drop(columns=[c for c in ["id"] if c in df_cli_hist.columns])
                st.dataframe(df_cli_hist, use_container_width=True)
                st.download_button(
                    "⬇️ Descargar historial CSV",
                    data=df_cli_hist.to_csv(index=False).encode("utf-8"),
                    file_name=f"historial_{sel_cliente_id}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

        st.divider()
        st.markdown("#### Historial global (solo lectura)")
        res_all = supabase.table("historial").select("*").order("fecha", desc=True).execute()
        hist = pd.DataFrame(res_all.data or [])
        st.dataframe(hist, use_container_width=True)

# =============================
# Footer
# =============================
st.markdown("---")
st.caption("Hecho por PiDBiM")
