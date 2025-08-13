# -*- coding: utf-8 -*-
# =============================================================================
# 📦 Streamlit 식자재 발주 시스템 (v2.2)
# - 주요 개선사항:
#   - 장바구니 추가 안정성 강화 및 불필요한 새로고침 방지 (st.rerun 제거)
#   - 사용자 피드백 강화를 위한 st.toast 메시지 적용
# =============================================================================

from io import BytesIO
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional
from collections.abc import Mapping
from zoneinfo import ZoneInfo

import hashlib
import pandas as pd
import streamlit as st

# Google Sheets
import gspread
from google.oauth2 import service_account

# Excel export
import xlsxwriter  # noqa: F401 (엔진 로딩용)

# -----------------------------------------------------------------------------
# 페이지/테마/스타일
# -----------------------------------------------------------------------------
st.set_page_config(page_title="산카쿠 식자재 발주 시스템", page_icon="📦", layout="wide")

THEME = {
    "BORDER": "#e8e8e8",
    "PRIMARY": "#1C6758",
    "BG": "#f7f8fa",
    "CARD_BG": "#ffffff",
    "TEXT": "#222",
    "MUTED": "#777",
}

CARD_STYLE = (
    f"background-color:{THEME['CARD_BG']};"
    f"border:1px solid {THEME['BORDER']};"
    f"border-radius:12px;padding:16px;"
)

st.markdown(f"""
<style>
html, body, [data-testid="stAppViewContainer"] {{
  background: {THEME['BG']};
  color: {THEME['TEXT']};
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans KR",
               "Apple SD Gothic Neo", "Malgun Gothic", "맑은 고딕", "Helvetica Neue", Arial, sans-serif;
}}
.small {{ font-size:12px; color:{THEME['MUTED']}; }}
.block-container {{ padding-top: 2.4rem; padding-bottom: 1.6rem; }}

.card {{ {CARD_STYLE} box-shadow: 0 2px 8px rgba(0,0,0,0.03); }}
.card-tight {{ background:{THEME['CARD_BG']}; border:1px solid {THEME['BORDER']}; border-radius:12px; padding:12px; }}
.metric {{ font-weight:700; color:{THEME['PRIMARY']}; }}

[data-testid="stAppViewContainer"] .main .block-container {{
  max-width: 1050px; margin: 0 auto; padding: 0 12px;
}}

.stTextInput>div>div>input,
.stNumberInput input,
.stDateInput input {{ border:1px solid {THEME['BORDER']} !important; border-radius:10px !important; height:34px; }}

.dataframe, .stDataFrame, .stTable {{
  background:{THEME['CARD_BG']}; border-radius:12px; border:1px solid {THEME['BORDER']};
}}
.dataframe td, .dataframe th {{ vertical-align: middle; }}

/* Tabs */
.stTabs [role="tablist"], div[role="tablist"] {{
  display:flex !important; gap:12px !important; flex-wrap:wrap !important;
  margin:8px 0 24px !important; border-bottom:none !important;
}}
.stTabs button[role="tab"], button[role="tab"] {{
  border:1px solid {THEME['BORDER']} !important; border-radius:12px !important; background:#fff !important;
  padding:10px 14px !important; box-shadow:0 1px 6px rgba(0,0,0,0.04) !important; cursor:pointer !important;
  transition: transform .08s ease, box-shadow .12s ease, border-color .12s ease, background-color .12s ease;
}}
.stTabs button[role="tab"]:hover, button[role="tab"]:hover {{
  transform: translateY(-1px); box-shadow:0 4px 12px rgba(0,0,0,0.08);
}}
.stTabs button[role="tab"][aria-selected="true"], button[role="tab"][aria-selected="true"] {{
  border-color:{THEME['PRIMARY']} !important; color:{THEME['PRIMARY']} !important;
  box-shadow:0 6px 16px rgba(28,103,88,0.18) !important; font-weight:700;
}}
.stTabs [data-baseweb="tab-highlight"], [data-baseweb="tab-highlight"] {{ display:none !important; }}

/* Sticky summary */
.sticky-bottom {{
  position: sticky; bottom: 0; z-index: 999;
  {CARD_STYLE}
  margin-top:10px; display:flex; align-items:center; justify-content:space-between; gap:16px;
}}

/* Title */
.login-title {{
  text-align:center; font-size:42px; font-weight:800; margin:16px 0 12px;
}}
.page-title {{ font-size:34px; font-weight:800; margin:12px 0; }}
.tabs-spacer {{ height: 10px; }}

/* 연한 회색 버튼 영역(전체선택/해제/삭제 등 유틸 버튼) */
.muted-buttons .stButton > button {{
  background: #f3f4f6 !important;
  color: #333 !important;
  border: 1px solid #e5e7eb !important;
}}
.muted-buttons .stButton > button:hover {{
  background: #e9eaee !important;
}}

/* 기본 버튼 스타일 */
.stButton > button[data-testid="baseButton-secondary"] {{
  background: #f3f4f6 !important;
  color: #333 !important;
  border: 1px solid #e5e7eb !important;
  border-radius: 10px !important;
  height: 34px !important;
}}
.stButton > button[data-testid="baseButton-secondary"]:hover {{
  background: #e9eaee !important;
}}

.stButton > button[data-testid="baseButton-primary"] {{
  background: #1C6758 !important;
  color: #fff !important;
  border: 1px solid #1C6758 !important;
  border-radius: 10px !important;
  height: 34px !important;
}}

/* ▶ 발주 수량 입력 섹션: 표/컨테이너 테두리·패딩 제거(박스안박스 제거) */
.flat-editor [data-testid="stDataFrame"],
.flat-editor [data-testid="stDataFrameContainer"],
.flat-editor [data-testid="stElementToolbar"],
.flat-editor .stDataFrame,
.flat-editor .dataframe,
.flat-editor .stTable {{
  background: transparent !important;
  border: none !important;
  box-shadow: none !important;
  border-radius: 0 !important;
}}
.flat-editor [data-testid="stVerticalBlock"] > div {{
  background: transparent !important;
  border: none !important;
  box-shadow: none !important;
  padding: 0 !important;
}}
.flat-editor [data-testid="stDataFrame"] > div {{
  background: transparent !important;
  border: none !important;
  box-shadow: none !important;
}}
</style>
""", unsafe_allow_html=True)

# --- 공용 작은 UI 유틸 ---
def fmt_num(x, decimals=0):
    try:
        if decimals == 0:
            return f"{float(x):,.0f}"
        return f"{float(x):,.{decimals}f}"
    except Exception:
        return "-"

def section_title(title: str, subtitle: str = ""):
    st.markdown(
        f"""
        <div class="card" style="padding:14px 16px;">
          <div style="font-size:22px; font-weight:800; color:{THEME['TEXT']};">{title}</div>
          {f'<div class="small" style="margin-top:4px;">{subtitle}</div>' if subtitle else ''}
        </div>
        """,
        unsafe_allow_html=True
    )

def info_chip(label: str, value: str):
    st.markdown(
        f"""<div class="card-tight" style="display:inline-flex; gap:8px; align-items:center; margin-right:8px;">
              <span class="small" style="color:{THEME['MUTED']};">{label}</span>
              <span class="metric">{value}</span>
            </div>""",
        unsafe_allow_html=True
    )

def card(html: str):
    st.markdown(f"""<div class="card">{html}</div>""", unsafe_allow_html=True)

def sticky_summary(left_html: str, right_html: str):
    st.markdown(
        f"""
        <div class="sticky-bottom">
          <div>{left_html}</div>
          <div style="font-weight:700; color:{THEME['PRIMARY']};">{right_html}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

# 공통 에디터 컬럼 설정
def numcol(label, step=1):
    return st.column_config.NumberColumn(label=label, min_value=0, step=step, format="%,d")

def textcol(label, help_txt=None):
    return st.column_config.TextColumn(label=label, help=help_txt or "")

# -----------------------------------------------------------------------------
# 시간/파일명 유틸(KST)
# -----------------------------------------------------------------------------
KST = ZoneInfo("Asia/Seoul")

def now_kst_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return datetime.now(KST).strftime(fmt)

def ymd(d: date) -> str:
    return d.strftime("%y%m%d")

def make_filename(prefix: str, dt_from: date, dt_to: date) -> str:
    return f"{prefix} {ymd(dt_from)}~{ymd(dt_to)}.xlsx"

# =============================================================================
# 1) Users 로더
# =============================================================================
def _normalize_account(uid: str, payload: Mapping) -> dict:
    pwd_plain = payload.get("password")
    pwd_hash  = payload.get("password_hash")
    name = str(payload.get("name", uid)).strip()
    role = str(payload.get("role", "store")).strip().lower()
    if not (pwd_plain or pwd_hash):
        st.error(f"[users.{uid}]에 password 또는 password_hash가 필요합니다."); st.stop()
    if role not in {"store", "admin"}:
        st.error(f"[users.{uid}].role 은 'store' 또는 'admin' 이어야 합니다. (현재: {role})"); st.stop()
    return {
        "password": (str(pwd_plain) if pwd_plain is not None else None),
        "password_hash": (str(pwd_hash).lower() if pwd_hash is not None else None),
        "name": name, "role": role,
    }

def load_users_from_secrets() -> Dict[str, Dict[str, str]]:
    cleaned: Dict[str, Dict[str, str]] = {}
    users_root = st.secrets.get("users", None)

    if isinstance(users_root, Mapping) and len(users_root) > 0:
        for uid, payload in users_root.items():
            if isinstance(payload, Mapping):
                cleaned[str(uid)] = _normalize_account(str(uid), payload)
    elif isinstance(users_root, list) and users_root:
        for row in users_root:
            if not isinstance(row, Mapping):
                continue
            uid = row.get("user_id") or row.get("uid") or row.get("id")
            if uid:
                cleaned[str(uid)] = _normalize_account(str(uid), row)

    if not cleaned:
        for uid in ("jeondae", "hq"):
            dotted_key = f"users.{uid}"
            payload = st.secrets.get(dotted_key, None)
            if isinstance(payload, Mapping):
                cleaned[str(uid)] = _normalize_account(str(uid), payload)
        if not cleaned:
            try:
                for k, v in dict(st.secrets).items():
                    if isinstance(k, str) and k.startswith("users.") and isinstance(v, Mapping):
                        uid = k.split(".", 1)[1].strip()
                        if uid:
                            cleaned[str(uid)] = _normalize_account(uid, v)
            except Exception:
                pass

    if not cleaned:
        with st.expander("🔍 Secrets 진단 (민감값 비노출)"):
            try:
                top_keys = list(dict(st.secrets).keys())
            except Exception:
                top_keys = []
            st.write({
                "has_users_section_as_mapping": isinstance(users_root, Mapping),
                "users_section_type": type(users_root).__name__,
                "top_level_keys": top_keys[:50],
            })
        st.error("로그인 계정을 찾을 수 없습니다. Secrets 의 [users.jeondae], [users.hq] 구조를 확인하세요.")
        st.stop()

    return cleaned

USERS = load_users_from_secrets()

# =============================================================================
# 2) 시트/스키마 정의
# =============================================================================
SHEET_NAME_MASTER = "상품마스터"
SHEET_NAME_ORDERS = "발주"
ORDER_STATUSES = ["접수", "출고완료"]
ORDERS_COLUMNS = [
    "주문일시","발주번호","지점ID","지점명","납품요청일",
    "품목코드","품목명","단위","수량","단가","금액",
    "비고","상태","처리일시","처리자"
]

# =============================================================================
# 3) Google Sheets 연결
# =============================================================================
def _require_google_secrets():
    google = st.secrets.get("google", {})
    required = ["type","project_id","private_key_id","private_key","client_email","client_id"]
    missing = [k for k in required if not str(google.get(k, "")).strip()]
    if missing:
        st.error("Google 연동 설정이 부족합니다. Secrets 의 [google] 섹션을 확인하세요.")
        st.write("누락 항목:", ", ".join(missing))
        st.stop()
    return google

@st.cache_resource(show_spinner=False)
def get_gs_client():
    google = _require_google_secrets()
    google = dict(google)
    pk = str(google.get("private_key", ""))
    if "\\n" in pk:
        google["private_key"] = pk.replace("\\n", "\n")
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_info(google, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def open_spreadsheet():
    g = st.secrets.get("google", {})
    key = str(g.get("SPREADSHEET_KEY") or st.secrets.get("SPREADSHEET_KEY", "")).strip()
    if not key:
        st.error("Secrets 에 SPREADSHEET_KEY가 없습니다. [google].SPREADSHEET_KEY 또는 루트 SPREADSHEET_KEY 설정 필요.")
        st.stop()
    try:
        return get_gs_client().open_by_key(key)
    except Exception as e:
        st.error(f"스프레드시트 열기 실패: {e}")
        st.stop()

# =============================================================================
# 4) 데이터 I/O
# =============================================================================
@st.cache_data(ttl=180)
def load_master_df() -> pd.DataFrame:
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_MASTER)
        df = pd.DataFrame(ws.get_all_records())
    except Exception:
        df = pd.DataFrame()
    if df.empty:
        df = pd.DataFrame([
            {"품목코드":"P001","품목명":"오이","단위":"EA","분류":"채소","단가":800,"활성":True},
            {"품목코드":"P002","품목명":"대파","단위":"KG","분류":"채소","단가":15600,"활성":True},
            {"품목코드":"P003","품목명":"간장","단위":"L","분류":"조미료","단가":3500,"활성":True},
        ])
    for c in ["품목코드","품목명","단위","분류","단가","활성"]:
        if c not in df.columns:
            df[c] = (0 if c=="단가" else (True if c=="활성" else ""))
    if "활성" in df.columns:
        mask = df["활성"].astype(str).str.lower().isin(["1","true","y","yes"])
        df = df[mask | df["활성"].isna()]
    return df

def write_master_df(df: pd.DataFrame) -> bool:
    cols = [c for c in ["품목코드","품목명","분류","단위","단가","활성"] if c in df.columns]
    df = df[cols].copy()
    try:
        sh = open_spreadsheet()
        try:
            ws = sh.worksheet(SHEET_NAME_MASTER)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=SHEET_NAME_MASTER, rows=2000, cols=25)
        ws.clear()
        values = [cols] + df.fillna("").values.tolist()
        ws.update("A1", values, value_input_option='USER_ENTERED')
        load_master_df.clear()
        return True
    except Exception as e:
        st.error(f"상품마스터 저장 실패: {e}")
        return False

@st.cache_data(ttl=120)
def load_orders_df() -> pd.DataFrame:
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_ORDERS)
        df = pd.DataFrame(ws.get_all_records())
    except Exception:
        df = pd.DataFrame()
    for c in ORDERS_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    df = df[ORDERS_COLUMNS].copy()
    return df

def write_orders_df(df: pd.DataFrame) -> bool:
    """[주의] 이 함수는 전체 시트를 덮어쓰므로, 상태 변경 등 전체 수정 시에만 사용해야 합니다."""
    df = df[ORDERS_COLUMNS].copy()
    try:
        sh = open_spreadsheet()
        try:
            ws = sh.worksheet(SHEET_NAME_ORDERS)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=SHEET_NAME_ORDERS, rows=5000, cols=25)
        ws.clear()
        values = [ORDERS_COLUMNS] + df.fillna("").values.tolist()
        ws.update("A1", values, value_input_option='USER_ENTERED')
        load_orders_df.clear() # 캐시 클리어
        return True
    except Exception as e:
        st.error(f"발주 저장 실패: {e}")
        return False

def append_orders(rows: List[Dict[str, Any]]) -> bool:
    """[개선] 경쟁 상태(Race Condition) 방지를 위해 기존 데이터를 덮어쓰지 않고 새로운 행만 추가합니다."""
    if not rows:
        return True
    try:
        sh = open_spreadsheet()
        try:
            ws = sh.worksheet(SHEET_NAME_ORDERS)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=SHEET_NAME_ORDERS, rows=5000, cols=25)
            ws.append_row(ORDERS_COLUMNS, value_input_option='USER_ENTERED')
        
        values_to_add = [ [r.get(col, "") for col in ORDERS_COLUMNS] for r in rows ]
        
        ws.append_rows(values_to_add, value_input_option='USER_ENTERED')
        load_orders_df.clear() # 데이터가 변경되었으므로 캐시를 지웁니다.
        return True
    except Exception as e:
        st.error(f"발주 추가 실패: {e}")
        return False

def update_order_status(selected_ids: List[str], new_status: str, handler: str) -> bool:
    df = load_orders_df().copy()
    if df.empty:
        st.warning("변경할 데이터가 없습니다."); return False
    now = now_kst_str()
    mask = df["발주번호"].astype(str).isin([str(x) for x in selected_ids])
    df.loc[mask, "상태"] = new_status
    df.loc[mask, "처리일시"] = now
    df.loc[mask, "처리자"] = handler
    return write_orders_df(df)

# =============================================================================
# 5) 로그인
# =============================================================================
def verify_password(input_pw: str, stored_hash: Optional[str], fallback_plain: Optional[str]) -> bool:
    if stored_hash:
        h = stored_hash.strip().lower()
        if h.startswith("sha256$"):
            h = h.split("$", 1)[1].strip()
        digest = hashlib.sha256(input_pw.encode()).hexdigest()
        return digest == h
    if fallback_plain is not None:
        return str(input_pw) == str(fallback_plain)
    return False

def _find_account(uid_or_name: str):
    s = str(uid_or_name or "").strip()
    if not s:
        return None, None
    lower_map = {k.lower(): k for k in USERS.keys()}
    if s in USERS:
        return s, USERS[s]
    if s.lower() in lower_map:
        real_uid = lower_map[s.lower()]
        return real_uid, USERS[real_uid]
    for uid, acct in USERS.items():
        nm = str(acct.get("name", "")).strip()
        if s == nm or s.lower() == nm.lower():
            return uid, acct
    return None, None

def _do_login(uid_input: str, pwd: str) -> bool:
    real_uid, acct = _find_account(uid_input)
    if not acct:
        st.error("아이디(또는 지점명) 또는 비밀번호가 올바르지 않습니다.")
        return False
    ok = verify_password(input_pw=pwd, stored_hash=acct.get("password_hash"), fallback_plain=acct.get("password"))
    if not ok:
        st.error("아이디(또는 지점명) 또는 비밀번호가 올바르지 않습니다.")
        return False
    st.session_state["auth"] = {"login": True, "user_id": real_uid, "name": acct["name"], "role": acct["role"]}
    st.success(f"{acct['name']}님 환영합니다!")
    st.rerun()
    return True

def require_login():
    st.session_state.setdefault("auth", {})
    if st.session_state["auth"].get("login", False):
        return True

    st.markdown("<div style='height:8vh'></div>", unsafe_allow_html=True)
    st.markdown('<div class="login-title">식자재 발주 시스템</div>', unsafe_allow_html=True)
    st.markdown("<div class='tabs-spacer'></div>", unsafe_allow_html=True)
    st.markdown("<div style='height:1vh'></div>", unsafe_allow_html=True)

    left, mid, right = st.columns([3, 2, 3], vertical_alignment="center")
    with mid:
        with st.form("login_form", clear_on_submit=False):
            uid = st.text_input("아이디 또는 지점명", key="login_uid", placeholder="예: jeondae / 전대점")
            pwd = st.text_input("비밀번호", type="password", key="login_pw")
            submitted = st.form_submit_button("로그인", use_container_width=True)
        if submitted:
            _do_login(uid, pwd)
    return False

# =============================================================================
# 6) 유틸
# =============================================================================
def make_order_id(store_id: str) -> str:
    return f"{datetime.now(KST):%Y%m%d%H%M%S}{store_id}"

def make_order_sheet_excel(df_note: pd.DataFrame, include_price: bool, *,
                           title: str = "산카쿠 납품내역서",
                           period_text: Optional[str] = None) -> BytesIO:
    buf = BytesIO()
    cols = ["발주번호","주문일시","납품요청일","지점명","품목코드","품목명","단위","수량","비고","상태"]
    if include_price:
        for c in ["단가","금액"]:
            if c not in df_note.columns:
                df_note[c] = 0
        cols += ["단가","금액"]

    export = df_note[cols].copy().sort_values(["발주번호","품목코드"])
    export["수량"] = pd.to_numeric(export.get("수량", 0), errors="coerce").fillna(0)
    if include_price:
        export["단가"] = pd.to_numeric(export.get("단가", 0), errors="coerce").fillna(0)
        export["금액"] = pd.to_numeric(export.get("금액", 0), errors="coerce").fillna(0)

    col_map = {}
    if include_price and "단가" in export.columns:
        col_map["단가"] = "단위당 단가"
    export = export.rename(columns=col_map)

    startrow = 4
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        export.to_excel(w, index=False, sheet_name="내역", startrow=startrow)
        wb = w.book
        ws = w.sheets["내역"]

        fmt_title = wb.add_format({"bold": True, "font_size": 16, "align":"center", "valign":"vcenter"})
        fmt_info  = wb.add_format({"font_size": 10})
        fmt_th    = wb.add_format({"bold": True, "bg_color":"#F2F2F2", "border":1})
        fmt_n     = wb.add_format({"num_format":"#,##0"})
        fmt_txt   = wb.add_format({})
        fmt_sum_l = wb.add_format({"bold": True})
        fmt_sum_n = wb.add_format({"bold": True, "num_format":"#,##0"})

        ncols = len(export.columns)
        ws.merge_range(0, 0, 0, ncols-1, title, fmt_title)
        ws.write(1, 0, f"조회기간: {period_text or ''}", fmt_info)
        ws.write(2, 0, f"생성일시(KST): {now_kst_str()}", fmt_info)

        for c in range(ncols):
            ws.write(startrow, c, export.columns[c], fmt_th)

        def col_idx(col_name: str) -> Optional[int]:
            try:
                return export.columns.get_loc(col_name)
            except Exception:
                return None

        idx_qty = col_idx("수량")
        if idx_qty is not None:
            ws.set_column(idx_qty, idx_qty, 10, fmt_n)
        if include_price:
            idx_unit = col_idx("단위당 단가"); idx_amt  = col_idx("금액")
            if idx_unit is not None: ws.set_column(idx_unit, idx_unit, 12, fmt_n)
            if idx_amt  is not None: ws.set_column(idx_amt, idx_amt, 14, fmt_n)

        auto_w = {"발주번호":16, "주문일시":19, "납품요청일":12, "지점명":12,
                  "품목코드":10, "품목명":18, "단위":8, "비고":18, "상태":10}
        for k, wth in auto_w.items():
            i = col_idx(k)
            if i is not None:
                ws.set_column(i, i, wth, fmt_txt)

        last_data_row = startrow + len(export)
        if idx_qty is not None:
            ws.write(last_data_row+1, max(idx_qty-1, 0), "총 수량", fmt_sum_l)
            ws.write(last_data_row+1, idx_qty, int(round(export["수량"].sum())), fmt_sum_n)
        if include_price and col_idx("금액") is not None:
            idx_amt = col_idx("금액")
            ws.write(last_data_row+1, idx_amt-1, "총 금액", fmt_sum_l)
            ws.write(last_data_row+1, idx_amt, int(round(export["금액"].sum())), fmt_sum_n)

    buf.seek(0)
    return buf

# =============================================================================
# 🛒 장바구니 유틸(전역)
# =============================================================================
def _ensure_cart():
    """세션에 cart DF가 없으면 초기화"""
    if "cart" not in st.session_state or not isinstance(st.session_state.get("cart"), pd.DataFrame):
        st.session_state["cart"] = pd.DataFrame(
            columns=["품목코드","품목명","단위","단가","수량","총금액"]
        )

def _coerce_price_qty(df: pd.DataFrame) -> pd.DataFrame:
    """단가/수량을 int로 강제, 총금액 재계산. 콤마/공백/문자/NaN 안전."""
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(columns=["품목코드","품목명","단위","단가","수량","총금액"])
    out = df.copy()
    for c in ["품목코드","품목명","단위","단가","수량","총금액"]:
        if c not in out.columns:
            out[c] = 0 if c in ("단가","수량","총금액") else ""
    out["단가"] = (
        pd.to_numeric(out["단가"].astype(str).str.replace(",", "").str.strip(), errors="coerce")
        .fillna(0).astype(int).clip(lower=0)
    )
    out["수량"] = (
        pd.to_numeric(out["수량"].astype(str).str.replace(",", "").str.strip(), errors="coerce")
        .fillna(0).astype(int).clip(lower=0)
    )
    out["총금액"] = (out["단가"] * out["수량"]).astype(int)
    return out[["품목코드","품목명","단위","단가","수량","총금액"]]

def normalize_cart(df: pd.DataFrame) -> pd.DataFrame:
    """0수량 제거한 정규화 장바구니 반환"""
    df = _coerce_price_qty(df)
    return df[df["수량"] > 0][["품목코드","품목명","단위","단가","수량","총금액"]].reset_index(drop=True)

def _add_to_cart(rows_df: pd.DataFrame):
    _ensure_cart()
    need_cols = ["품목코드","품목명","단위","단가","수량"]
    if not isinstance(rows_df, pd.DataFrame) or any(c not in rows_df.columns for c in need_cols):
        return
    add = _coerce_price_qty(rows_df[need_cols].copy())
    add = add[add["수량"] > 0]
    if add.empty:
        return

    cart = _coerce_price_qty(st.session_state["cart"]).copy()
    add["__new__"]  = 1
    cart["__new__"] = 0
    merged = pd.concat([cart, add], ignore_index=True, sort=False).sort_values(["품목코드","__new__"])

    agg = merged.groupby("품목코드", as_index=False).agg({
        "품목명": "last", "단위": "last", "단가": "last", "수량": "sum",
    })
    agg["총금액"] = (agg["단가"] * agg["수량"])
    st.session_state["cart"] = agg[["품목코드","품목명","단위","단가","수량","총금액"]]

def _remove_from_cart(codes: list[str]):
    _ensure_cart()
    if not codes:
        return
    codes_to_remove = set(map(str, codes))
    cart_df = st.session_state["cart"]
    cart_df = cart_df[~cart_df["품목코드"].astype(str).isin(codes_to_remove)]
    st.session_state["cart"] = cart_df.reset_index(drop=True)

def _clear_cart():
    st.session_state["cart"] = pd.DataFrame(
        columns=["품목코드","품목명","단위","단가","수량","총금액"]
    )

# ──────────────────────────────────────────────
# 🛒 발주(지점) 화면 (수정됨)
# ──────────────────────────────────────────────
def page_store_register_confirm(master_df: pd.DataFrame):
    # 세션 상태 초기화
    _ensure_cart()
    if "store_editor_ver" not in st.session_state:
        st.session_state.store_editor_ver = 0
    if "cart_selected_codes" not in st.session_state:
        st.session_state.cart_selected_codes = []
    
    st.subheader("🛒 발주 요청")

    today, max_day = date.today(), date.today() + timedelta(days=7)

    with st.container(border=True):
        st.markdown("### 🗓️ 납품 요청 정보")
        c1, c2 = st.columns([1, 1])
        quick = c1.radio("납품 선택", ["오늘", "내일", "직접선택"], horizontal=True, key="store_quick_radio")
        
        if quick == "오늘": 납품요청일 = today
        elif quick == "내일": 납품요청일 = min(today + timedelta(days=1), max_day)
        else:
            default = st.session_state.get("store_req_date", today)
            if not isinstance(default, date) or not (today <= default <= max_day): default = today
            납품요청일 = c2.date_input("납품 요청일", value=default, min_value=today, max_value=max_day, key="store_req_date")
        
        memo = st.text_area("요청 사항(선택)", key="store_req_memo", height=80, placeholder="예) 입고 시 얼음팩 추가 부탁드립니다.")

    df_master = master_df.copy()
    df_master["단가"] = pd.to_numeric(df_master.get("단가", 0), errors="coerce").fillna(0).astype(int)

    with st.container(border=True):
        st.markdown("### 🧾 발주 수량 입력")
        l, r = st.columns([2, 1])
        keyword = l.text_input("품목 검색(이름/코드)", key="store_kw")
        cat_opt = ["(전체)"] + sorted(df_master["분류"].dropna().unique().tolist())
        cat_sel = r.selectbox("분류(선택)", cat_opt, key="store_cat_sel")

        df_view = df_master.copy()
        if keyword:
            q = keyword.strip().lower()
            df_view = df_view[df_view.apply(lambda row: q in str(row["품목명"]).lower() or q in str(row["품목코드"]).lower(), axis=1)]
        if cat_sel != "(전체)":
            df_view = df_view[df_view["분류"] == cat_sel]

        editor_key = f"store_order_editor_v{st.session_state.store_editor_ver}"
        
        st.markdown("<div class='flat-editor'>", unsafe_allow_html=True)
        # data_editor를 폼으로 감싸서 버튼 클릭 시 데이터가 안정적으로 제출되도록 함
        with st.form(key="add_to_cart_form"):
            df_edit_disp = df_view[["품목코드","품목명","단위","단가"]].copy()
            df_edit_disp["단가(원)"] = df_edit_disp["단가"].map(lambda v: f"{v:,.0f}")
            df_edit_disp["수량"] = ""
            
            edited_disp = st.data_editor(
                df_edit_disp[["품목코드","품목명","단위","단가(원)","수량"]],
                key=editor_key,
                disabled=["품목코드","품목명","단위","단가(원)"],
                hide_index=True, use_container_width=True, num_rows="fixed", height=380,
            )
            add_clicked = st.form_submit_button("장바구니 추가", use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        # [수정] 장바구니 추가 로직
        if add_clicked:
            items_to_add = normalize_cart(edited_disp)
            if items_to_add.empty:
                st.warning("수량이 0보다 큰 품목이 없습니다.")
            else:
                base = df_view[["품목코드","품목명","단위","단가"]].copy()
                final_add_data = items_to_add[["품목코드", "수량"]].merge(base, on="품목코드", how="left")
                
                _add_to_cart(final_add_data)
                st.toast(f"{len(items_to_add)}개 품목을 장바구니에 추가했습니다.", icon="🛒")
                st.session_state.store_editor_ver += 1 # 입력창 초기화를 위해 key 변경
                st.experimental_rerun() # 안정적인 새로고침

    with st.container(border=True):
        st.markdown("### 🧺 장바구니")
        # 장바구니 수정 로직
        cart = st.session_state.get("cart", pd.DataFrame())
        if not cart.empty:
            cart_disp = cart.copy()
            cart_disp.insert(0, "선택", cart_disp["품목코드"].astype(str).isin(st.session_state.cart_selected_codes))
            cart_disp["수량"] = cart_disp["수량"].astype(str) # TextColumn을 위해 문자열로 변경
            
            edited_cart = st.data_editor(
                cart_disp[["선택","품목코드","품목명","단위","수량","단가","총금액"]],
                key="cart_editor",
                disabled=["품목코드","품목명","단위","총금액"],
                hide_index=True, use_container_width=True, height=340
            )
            
            # 변경사항 즉시 세션 상태에 반영
            st.session_state.cart_selected_codes = edited_cart[edited_cart["선택"]]["품목코드"].astype(str).tolist()
            updated_cart_df = edited_cart.drop(columns=["선택"])
            st.session_state.cart = normalize_cart(updated_cart_df)
            
            # 장바구니 관리 버튼
            st.markdown("<div class='muted-buttons'>", unsafe_allow_html=True)
            c1, c2, c3 = st.columns([1,1,1])
            all_codes = st.session_state.cart["품목코드"].astype(str).tolist()
            is_all_selected = set(st.session_state.cart_selected_codes) == set(all_codes) and all_codes
            
            if c1.button("전체 해제" if is_all_selected else "전체 선택", use_container_width=True):
                st.session_state.cart_selected_codes = [] if is_all_selected else all_codes
                st.experimental_rerun()
            if c2.button("선택 삭제", use_container_width=True):
                _remove_from_cart(st.session_state.cart_selected_codes)
                st.session_state.cart_selected_codes = []
                st.experimental_rerun()
            if c3.button("장바구니 비우기", use_container_width=True):
                _clear_cart()
                st.session_state.cart_selected_codes = []
                st.experimental_rerun()
            st.markdown("</div>", unsafe_allow_html=True)

        else:
            st.info("장바구니가 비어 있습니다.")

    cart_now = st.session_state.get("cart", pd.DataFrame())
    total_items, total_qty, total_amt = len(cart_now), cart_now["수량"].sum(), cart_now["총금액"].sum()
    
    sticky_summary(f"납품 요청일: <b>{납품요청일:%Y-%m-%d}</b>", f"선택 품목수: <span class='metric'>{total_items:,}</span> 개 &nbsp;&nbsp; 총 수량: <span class='metric'>{total_qty:,}</span> &nbsp;&nbsp; 총 금액: <span class='metric'>{total_amt:,.0f}</span> 원")

    with st.form("submit_form"):
        confirm = st.checkbox("제출 전 입력 내용 확인했습니다.", value=False)
        submitted = st.form_submit_button("📦 발주 제출", type="primary", use_container_width=True)

        if submitted:
            if total_items == 0:
                st.warning("장바구니가 비어 있습니다."); st.stop()
            if not confirm:
                st.warning("체크박스를 확인해 주세요."); st.stop()

            user, order_id, now = st.session_state["auth"], make_order_id(st.session_state["auth"]["user_id"]), now_kst_str()
            rows = [
                {**r.to_dict(), "주문일시": now, "발주번호": order_id, "지점ID": user["user_id"], "지점명": user["name"], "납품요청일": f"{납품요청일:%Y-%m-%d}", "비고": memo, "상태": "접수", "처리일시": "", "처리자": ""}
                for _, r in cart_now.iterrows()
            ]
            
            if append_orders(rows):
                st.success(f"발주가 접수되었습니다. 발주번호: {order_id}")
                _clear_cart()
                st.session_state.cart_selected_codes = []
                st.experimental_rerun()
            else:
                st.error("발주 저장에 실패했습니다.")

# ──────────────────────────────────────────────
# 이하 페이지 함수들은 이전 버전과 동일하게 유지됩니다.
# (st.rerun()을 st.experimental_rerun()으로 변경하여 안정성을 높일 수 있습니다.)
# ──────────────────────────────────────────────

def page_store_orders_change():
    st.subheader("🧾 발주 조회 · 수정")
    if "orders_selected_ids" not in st.session_state:
        st.session_state.orders_selected_ids = []

    df_all = load_orders_df().copy()
    user = st.session_state["auth"]
    df_user = df_all[df_all["지점ID"].astype(str) == user.get("user_id")]
    if df_user.empty:
        st.info("발주 데이터가 없습니다."); return

    with st.container(border=True):
        st.markdown("### 🔎 조회 조건")
        c1, c2 = st.columns(2)
        dt_from = c1.date_input("시작일", date.today() - timedelta(days=14), key="store_edit_from")
        dt_to = c2.date_input("종료일", date.today(), key="store_edit_to")

    dt_series = pd.to_datetime(df_user["주문일시"], errors="coerce").dt.date
    df = df_user[(dt_series >= dt_from) & (dt_series <= dt_to)].copy()
    
    if df.empty:
        st.info("해당 기간에 조회할 발주가 없습니다."); return

    orders = df.groupby("발주번호").agg(
        주문일시=("주문일시", "first"), 건수=("품목코드", "count"),
        총수량=("수량", lambda x: pd.to_numeric(x, 'coerce').sum()),
        총금액=("금액", lambda x: pd.to_numeric(x, 'coerce').sum()), 상태=("상태", "first")
    ).reset_index().sort_values("주문일시", ascending=False)
    
    orders_pending = orders[orders["상태"] == "접수"]
    orders_done = orders[orders["상태"] == "출고완료"]
    
    with st.container(border=True):
        st.markdown("### 📦 발주 리스트")
        st.markdown("**접수(삭제 가능)**")
        if not orders_pending.empty:
            pend_disp = orders_pending.copy()
            pend_disp.insert(0, "선택", pend_disp["발주번호"].isin(st.session_state.orders_selected_ids))
            edited_pending = st.data_editor(
                pend_disp, key="store_pending_editor", use_container_width=True, hide_index=True, height=240,
                disabled=["발주번호", "주문일시", "건수", "총수량", "총금액", "상태"]
            )
            st.session_state.orders_selected_ids = edited_pending[edited_pending["선택"]]["발주번호"].tolist()
            
            c1, c2 = st.columns([1, 4])
            if c1.button("선택 발주 삭제", key="btn_pend_delete", type="primary"):
                if st.session_state.orders_selected_ids:
                    base = load_orders_df()
                    to_keep = base[~base["발주번호"].isin(st.session_state.orders_selected_ids)]
                    if write_orders_df(to_keep):
                        st.success("선택한 발주를 삭제했습니다.")
                        st.session_state.orders_selected_ids = []
                        st.experimental_rerun()
                    else:
                        st.error("삭제 실패")
                else:
                    st.warning("삭제할 발주를 선택하세요.")
        else:
            st.info("접수 상태의 발주가 없습니다.")
        
        st.markdown("---")
        st.markdown("**출고완료(수정불가)**")
        if not orders_done.empty:
            done_disp = orders_done.copy()
            done_disp.insert(0, "선택", done_disp["발주번호"].isin(st.session_state.orders_selected_ids))
            edited_done = st.data_editor(
                done_disp, key="store_done_editor", use_container_width=True, hide_index=True, height=200,
                disabled=["발주번호", "주문일시", "건수", "총수량", "총금액", "상태"]
            )
            # 출고완료 건 선택 시, 다른 선택은 해제
            selected_done = edited_done[edited_done["선택"]]["발주번호"].tolist()
            if selected_done:
                st.session_state.orders_selected_ids = selected_done
        else:
            st.info("출고완료된 발주가 없습니다.")

    with st.container(border=True):
        st.markdown("### 📄 발주품목조회")
        if len(st.session_state.orders_selected_ids) == 1:
            target_order = st.session_state.orders_selected_ids[0]
            target_df = df_user[df_user["발주번호"] == target_order].copy()
            st.caption(f"발주번호: {target_order}")
            st.dataframe(target_df[ORDERS_COLUMNS[5:12]], use_container_width=True, hide_index=True)
        else:
            st.info("위 목록에서 상세 내용을 확인할 발주를 하나만 선택하세요.")

def page_store_order_form_download(master_df: pd.DataFrame):
    st.subheader("📑 발주서 다운로드")
    
    df_all = load_orders_df().copy()
    user = st.session_state["auth"]
    df = df_all[df_all["지점ID"].astype(str) == user.get("user_id")]
    if df.empty:
        st.info("발주 데이터가 없습니다."); return

    with st.container(border=True):
        st.markdown("### 🔎 조회 조건")
        c1, c2, c3 = st.columns([1, 1, 2])
        dt_from = c1.date_input("시작일", date.today() - timedelta(days=7), key="store_dl_from")
        dt_to = c2.date_input("종료일", date.today(), key="store_dl_to")
        order_ids = ["(전체)"] + sorted(df["발주번호"].dropna().unique().tolist(), reverse=True)
        target_order = c3.selectbox("발주번호(선택 시 해당 건만)", order_ids, key="store_dl_orderid")
    
    dt_series = pd.to_datetime(df["주문일시"], errors="coerce").dt.date
    mask = (dt_series >= dt_from) & (dt_series <= dt_to)
    if target_order != "(전체)":
        mask &= (df["발주번호"] == target_order)
    dfv = df[mask].copy().sort_values(["발주번호", "품목코드"])

    with st.container(border=True):
        st.markdown("### 📄 미리보기")
        st.dataframe(dfv, use_container_width=True, height=420,
            column_config={"단가": numcol("단가(원)"), "금액": numcol("금액(원)"), "수량": numcol("수량")})
        
        if not dfv.empty:
            period_text = f"{dt_from:%Y-%m-%d} ~ {dt_to:%Y-%m-%d}"
            buf = make_order_sheet_excel(dfv, include_price=False, title="산카쿠 발주서", period_text=period_text)
            fname = make_filename("산카쿠 발주서", dt_from, dt_to)
            st.download_button("발주서 엑셀 다운로드", data=buf.getvalue(), file_name=fname, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="store_dl_btn", use_container_width=True)

def page_store_master_view(master_df: pd.DataFrame):
    st.subheader("🏷️ 발주 품목 가격 조회")
    cols = ["품목코드", "품목명", "분류", "단위", "단가"]
    view = master_df[[c for c in cols if c in master_df.columns]].copy()
    view["단가"] = pd.to_numeric(view.get("단가", 0), errors="coerce").fillna(0).astype(int)
    with st.container(border=True):
        st.markdown("### 📋 품목 리스트")
        st.dataframe(view, use_container_width=True, height=520, column_config={"단가": numcol("단가(원)")})

def page_admin_unified_management():
    st.subheader("🗂️ 발주요청조회 · 수정")
    
    if "admin_pending_selection" not in st.session_state: st.session_state.admin_pending_selection = []
    if "admin_shipped_selection" not in st.session_state: st.session_state.admin_shipped_selection = []

    df_all = load_orders_df().copy()
    if df_all.empty: st.info("발주 데이터가 없습니다."); return

    with st.container(border=True):
        st.markdown("### 🔎 조회 조건")
        c1, c2, c3 = st.columns([1,1,2])
        dt_from = c1.date_input("시작일", date.today()-timedelta(days=7), key="admin_mng_from")
        dt_to = c2.date_input("종료일", date.today(), key="admin_mng_to")
        stores = ["(전체)"] + sorted(df_all["지점명"].dropna().unique().tolist())
        store = c3.selectbox("지점", stores, key="admin_mng_store")

    dt_series = pd.to_datetime(df_all["주문일시"], errors="coerce").dt.date
    mask = (dt_series >= dt_from) & (dt_series <= dt_to)
    if store != "(전체)": mask &= (df_all["지점명"] == store)
    df = df_all[mask].copy()

    orders = df.groupby("발주번호").agg(
        주문일시=("주문일시", "first"), 지점명=("지점명", "first"),
        건수=("품목코드", "count"), 총수량=("수량", lambda x: pd.to_numeric(x, 'coerce').sum()),
        총금액=("금액", lambda x: pd.to_numeric(x, 'coerce').sum()), 상태=("상태", "first")
    ).reset_index().sort_values("주문일시", ascending=False)
    
    orders_pending = orders[orders["상태"] == "접수"]
    orders_shipped = orders[orders["상태"] == "출고완료"]
    
    tab1, tab2 = st.tabs([f"📦 발주 요청 접수 ({len(orders_pending)}건)", f"✅ 출고 완료 ({len(orders_shipped)}건)"])
    
    with tab1:
        if not orders_pending.empty:
            pend_disp = orders_pending.copy()
            pend_disp.insert(0, "선택", pend_disp["발주번호"].isin(st.session_state.admin_pending_selection))
            edited_pending = st.data_editor(pend_disp, key="admin_pending_editor", use_container_width=True, hide_index=True, disabled=orders.columns.drop("발주번호").tolist())
            st.session_state.admin_pending_selection = edited_pending[edited_pending["선택"]]["발주번호"].tolist()
            if st.button("✅ 선택 발주 출고", key="btn_pend_ship", type="primary"):
                if st.session_state.admin_pending_selection:
                    if update_order_status(st.session_state.admin_pending_selection, "출고완료", st.session_state.auth.get("name","관리자")):
                        st.success("출고완료 처리되었습니다."); st.session_state.admin_pending_selection = []; st.experimental_rerun()
                else: st.warning("출고할 발주를 선택하세요.")
        else: st.info("접수 상태인 발주가 없습니다.")

    with tab2:
        if not orders_shipped.empty:
            ship_disp = orders_shipped.copy()
            ship_disp.insert(0, "선택", ship_disp["발주번호"].isin(st.session_state.admin_shipped_selection))
            edited_shipped = st.data_editor(ship_disp, key="admin_shipped_editor", use_container_width=True, hide_index=True, disabled=orders.columns.drop("발주번호").tolist())
            st.session_state.admin_shipped_selection = edited_shipped[edited_shipped["선택"]]["발주번호"].tolist()
            if st.button("↩️ 접수 상태로 변경", key="btn_ship_revert"):
                if st.session_state.admin_shipped_selection:
                    if update_order_status(st.session_state.admin_shipped_selection, "접수", st.session_state.auth.get("name","관리자")):
                        st.success("접수 상태로 변경되었습니다."); st.session_state.admin_shipped_selection = []; st.experimental_rerun()
                else: st.warning("상태를 변경할 발주를 선택하세요.")
        else: st.info("출고 완료된 발주가 없습니다.")
            
    with st.container(border=True):
        st.markdown("### 📄 발주요청품목확인")
        total_selection = st.session_state.admin_pending_selection + st.session_state.admin_shipped_selection
        if len(total_selection) == 1:
            target_order = total_selection[0]
            target_df = df_all[df_all["발주번호"] == target_order].copy()
            st.caption(f"발주번호: {target_order} | 지점명: {target_df['지점명'].iloc[0]} | 상태: {target_df['상태'].iloc[0]}")
            st.dataframe(target_df[ORDERS_COLUMNS[5:12]], use_container_width=True, hide_index=True)
            period_text = f"{target_df['납품요청일'].iloc[0]} ({target_df['지점명'].iloc[0]})"
            buf = make_order_sheet_excel(target_df, include_price=True, title="산카쿠 납품내역서", period_text=period_text)
            st.download_button("해당 건의 출고 내역 다운로드", data=buf.getvalue(), file_name=f"납품내역서_{target_order}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.info("위 목록에서 상세 내용을 확인할 발주를 하나만 선택하세요.")

def page_admin_delivery_note(master_df: pd.DataFrame):
    st.subheader("📑 출고 내역서 다운로드")
    df = load_orders_df().copy()
    if df.empty: st.info("발주 데이터가 없습니다."); return
    with st.container(border=True):
        st.markdown("### 🔎 조회 조건")
        c1, c2, c3 = st.columns([1,1,2])
        dt_from = c1.date_input("시작일", date.today()-timedelta(days=7), key="admin_note_from")
        dt_to = c2.date_input("종료일", date.today(), key="admin_note_to")
        stores = ["(전체)"] + sorted(df["지점명"].dropna().unique().tolist())
        store = c3.selectbox("지점(선택)", stores, key="admin_note_store")

    dt_series = pd.to_datetime(df["주문일시"], errors="coerce").dt.date
    mask = (dt_series >= dt_from) & (dt_series <= dt_to)
    if store != "(전체)": mask &= (df["지점명"]==store)
    dfv = df[mask].copy().sort_values(["지점명", "발주번호", "품목코드"])

    with st.container(border=True):
        st.markdown("### 📄 미리보기")
        st.dataframe(dfv, use_container_width=True, height=420)
        if not dfv.empty:
            period_text = f"{dt_from:%Y-%m-%d} ~ {dt_to:%Y-%m-%d}" + ("" if store=="(전체)" else f" | 지점: {store}")
            buf = make_order_sheet_excel(dfv, include_price=True, title="산카쿠 납품내역서", period_text=period_text)
            fname = make_filename(f"산카쿠_납품내역서_{store if store != '(전체)' else '전체'}", dt_from, dt_to)
            st.download_button("출고 내역서 엑셀 다운로드", data=buf.getvalue(), file_name=fname, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="admin_note_btn", use_container_width=True)

def page_admin_items_price(master_df: pd.DataFrame):
    st.subheader("🏷️ 납품 품목 가격 설정")
    st.caption("단가·활성 여부를 수정하거나, 품목을 새로 추가/삭제한 후 [변경사항 저장]을 누르세요.")
    
    cols = ["품목코드","품목명","분류","단위","단가","활성"]
    view = master_df[[c for c in cols if c in master_df.columns]].copy()
    view["삭제"] = False

    with st.container(border=True):
        st.markdown("### ✏️ 품목 리스트 편집")
        edited = st.data_editor(
            view, use_container_width=True, hide_index=True, num_rows="dynamic",
            column_config={"단가": numcol("단위당 단가"), "활성": st.column_config.CheckboxColumn(default=True), "삭제": st.column_config.CheckboxColumn(default=False)},
            key="admin_master_editor"
        )
        if st.button("변경사항 저장", type="primary", use_container_width=True, key="admin_master_save"):
            final_df = edited[~edited["삭제"].fillna(False)].drop(columns=["삭제"])
            final_df["단가"] = pd.to_numeric(final_df["단가"], errors="coerce").fillna(0).astype(int)
            if write_master_df(final_df):
                st.success("상품마스터에 저장되었습니다.")
                st.cache_data.clear()
                st.experimental_rerun()
            else:
                st.error("저장 실패")

# =============================================================================
# 라우팅
# =============================================================================
if __name__ == "__main__":
    if not require_login():
        st.stop()

    st.title("📦 식자재 발주 시스템")

    user = st.session_state["auth"]
    role = user.get("role", "store")
    master = load_master_df()

    if role == "admin":
        t1, t2, t3 = st.tabs(["🗂️ 발주요청조회·수정", "📑 출고 내역서 다운로드", "🏷️ 납품 품목 가격 설정"])
        with t1: page_admin_unified_management()
        with t2: page_admin_delivery_note(master)
        with t3: page_admin_items_price(master)
    else:
        t1, t2, t3, t4 = st.tabs(["🛒 발주 요청", "🧾 발주 조회·수정", "📑 발주서 다운로드", "🏷️ 발주 품목 가격 조회"])
        with t1: page_store_register_confirm(master)
        with t2: page_store_orders_change()
        with t3: page_store_order_form_download(master)
        with t4: page_store_master_view(master)
