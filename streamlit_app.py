# -*- coding: utf-8 -*-
# =============================================================================
# 📦 Streamlit 식자재 발주 시스템 (최적화/오류수정/UX통일판)
# - 수량 입력(TextColumn + 콤마 허용) / 버튼 1회 반영 / 장바구니 누적 / 박스안박스 제거
# - 납품일: 오늘~7일 이내만 선택(과거/8일 이후 제한)
# - 발주요청 네이밍 통일 / 리스트 선택·삭제 메커니즘 장바구니와 통일 / 새로고침 최소화
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
          {'<div class="small" style="margin-top:4px;">'+subtitle+'</div>' if subtitle else ''}
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

EDITOR_CFG = {
    "품목코드": st.column_config.TextColumn(label="품목코드"),
    "품목명":   st.column_config.TextColumn(label="품목명"),
    "단위":     st.column_config.TextColumn(label="단위"),
    "수량_num": st.column_config.NumberColumn(label="수량", min_value=0, step=1),
    "수량_txt": st.column_config.TextColumn(label="수량", help="숫자/콤마 모두 입력 가능"),
    "단가":     st.column_config.NumberColumn(label="단가(원)", min_value=0, step=1, format="%,d"),
    "총금액":   st.column_config.NumberColumn(label="총금액(원)", min_value=0, step=1, format="%,d"),
}

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
        except Exception:
            ws = sh.add_worksheet(title=SHEET_NAME_MASTER, rows=2000, cols=25)
        ws.clear()
        values = [cols] + df.fillna("").values.tolist()
        ws.update("A1", values)
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
    df = df[ORDERS_COLUMNS].copy()
    try:
        sh = open_spreadsheet()
        try:
            ws = sh.worksheet(SHEET_NAME_ORDERS)
        except Exception:
            ws = sh.add_worksheet(title=SHEET_NAME_ORDERS, rows=5000, cols=25)
        ws.clear()
        values = [ORDERS_COLUMNS] + df.fillna("").values.tolist()
        ws.update("A1", values)
        load_orders_df.clear()
        return True
    except Exception as e:
        st.error(f"발주 저장 실패: {e}")
        return False

def append_orders(rows: List[Dict[str, Any]]) -> bool:
    base = load_orders_df()
    df_new = pd.DataFrame(rows)[ORDERS_COLUMNS]
    return write_orders_df(pd.concat([base, df_new], ignore_index=True))

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
    # YYYYMMDDHHMMSS + 지점ID (초 포함)
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
# 🛒 장바구니 유틸(전역) — 반드시 이 아래부터 페이지 함수에서 사용
# =============================================================================
def _ensure_cart():
    """세션에 cart DF가 없으면 초기화"""
    if "cart" not in st.session_state or not isinstance(st.session_state.get("cart"), pd.DataFrame):
        st.session_state["cart"] = pd.DataFrame(
            columns=["품목코드","품목명","단위","단가","수량","총금액"]
        )

def _coerce_price_qty(df: pd.DataFrame) -> pd.DataFrame:
    """단가/수량을 int로 강제, 총금액 재계산. 콤마/공백/문자/NaN 안전."""
    if not isinstance(df, pd.DataFrame):
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
    return df[df["수량"] > 0][["품목코드","품목명","단위","단가","수량","총금액"]]

def _add_to_cart(rows_df: pd.DataFrame):
    """
    장바구니에 안전하게 '누적' 추가.
    - 같은 품목코드는 수량 합산
    - 품목명/단위/단가는 '최근 추가분'으로 갱신
    """
    _ensure_cart()
    need_cols = ["품목코드","품목명","단위","단가","수량"]
    if not isinstance(rows_df, pd.DataFrame) or any(c not in rows_df.columns for c in need_cols):
        return
    add = _coerce_price_qty(rows_df[need_cols].copy())
    add = add[add["수량"] > 0]
    if add.empty:
        return

    cart = _coerce_price_qty(st.session_state["cart"]).copy()
    add["__new__"]  = 1  # 최근 추가분 표시
    cart["__new__"] = 0
    merged = pd.concat([cart, add], ignore_index=True, sort=False).sort_values(["품목코드","__new__"])

    agg = merged.groupby("품목코드", as_index=False).agg({
        "품목명": "last",
        "단위":   "last",
        "단가":   "last",
        "수량":   "sum",
    })
    agg["총금액"] = (pd.to_numeric(agg["단가"], errors="coerce").fillna(0).astype(int) *
                   pd.to_numeric(agg["수량"], errors="coerce").fillna(0).astype(int)).astype(int)

    st.session_state["cart"] = agg[["품목코드","품목명","단위","단가","수량","총금액"]]

def _remove_from_cart(codes: list[str]):
    _ensure_cart()
    if not codes:
        return
    codes = [str(c) for c in codes]
    st.session_state["cart"] = st.session_state["cart"][
        ~st.session_state["cart"]["품목코드"].astype(str).isin(codes)
    ]

def _clear_cart():
    st.session_state["cart"] = pd.DataFrame(
        columns=["품목코드","품목명","단위","단가","수량","총금액"]
    )

# ──────────────────────────────────────────────
# 🛒 발주(지점) 화면 — 수량입력 자유(텍스트) + 박스중복 제거 + 장바구니 체크박스/오토세이브
# ──────────────────────────────────────────────
def page_store_register_confirm(master_df: pd.DataFrame):
    # 세션
    _ensure_cart()
    st.session_state.setdefault("store_editor_ver", 0)
    st.session_state.setdefault("cart_selected_codes", [])  # 장바구니 체크 상태

    # 제목
    st.subheader("🛒 발주 요청")

    # 납품일 제한: 오늘 ~ 7일 이내
    today = date.today()
    max_day = today + timedelta(days=7)

    # 1) 납품 요청 정보
    with st.container(border=True):
        st.markdown("### 🗓️ 납품 요청 정보")
        c1, c2 = st.columns([1, 1])
        with c1:
            quick = st.radio("납품 선택", ["오늘", "내일", "직접선택"], horizontal=True, key="store_quick_radio")
        with c2:
            if quick == "오늘":
                납품요청일 = today
            elif quick == "내일":
                납품요청일 = min(today + timedelta(days=1), max_day)
            else:
                default = min(max(st.session_state.get("store_req_date", today), today), max_day) \
                          if isinstance(st.session_state.get("store_req_date"), date) else today
                납품요청일 = st.date_input(
                    "납품 요청일", value=default, min_value=today, max_value=max_day, key="store_req_date"
                )
        # 보정(직접선택 외 케이스에서도 강제 범위)
        if not (today <= 납품요청일 <= max_day):
            납품요청일 = min(max(납품요청일, today), max_day)

        memo = st.text_area("요청 사항(선택)", key="store_req_memo", height=80,
                            placeholder="예) 입고 시 얼음팩 추가 부탁드립니다.")

    # 마스터(단가 정수화)
    df_master = master_df.copy()
    df_master["단가"] = pd.to_numeric(df_master.get("단가", 0), errors="coerce").fillna(0).astype(int)

    # 2) 발주 수량 입력 — 검색 + 표 + 버튼(폼 없음, 겉박스 1개만)
    with st.container(border=True):
        st.markdown("### 🧾 발주 수량 입력")

        # 검색행
        l, r = st.columns([2, 1])
        with l:
            keyword = st.text_input("품목 검색(이름/코드)", key="store_kw")
        with r:
            if "분류" in df_master.columns:
                cat_opt = ["(전체)"] + sorted(df_master["분류"].dropna().unique().tolist())
                cat_sel = st.selectbox("분류(선택)", cat_opt, key="store_cat_sel")
            else:
                cat_sel = "(전체)"

        # 필터링
        df_view = df_master.copy()
        if keyword:
            q = keyword.strip().lower()
            df_view = df_view[df_view.apply(
                lambda row: q in str(row.get("품목명","")).lower()
                         or q in str(row.get("품목코드","")).lower(), axis=1)]
        if "분류" in df_master.columns and cat_sel != "(전체)":
            df_view = df_view[df_view["분류"] == cat_sel]

        # 표 (폼 없이) — 안쪽 박스 제거용 .flat-editor 래퍼
        df_edit_disp = df_view[["품목코드","품목명","단위","단가"]].copy()
        df_edit_disp["단가(원)"] = df_edit_disp["단가"].map(lambda v: f"{v:,.0f}")
        df_edit_disp["수량"] = ""   # 콤마 허용(TextColumn)
        editor_key = f"store_order_editor_v{st.session_state['store_editor_ver']}"

        st.markdown("<div class='flat-editor'>", unsafe_allow_html=True)
        edited_disp = st.data_editor(
            df_edit_disp[["품목코드","품목명","단위","단가(원)","수량"]],
            column_config={
                "수량":     st.column_config.TextColumn(label="수량", help="숫자/콤마 입력 가능"),
                "단가(원)": st.column_config.TextColumn(label="단가(원)"),
                "품목코드": st.column_config.TextColumn(label="품목코드"),
                "품목명":   st.column_config.TextColumn(label="품목명"),
                "단위":     st.column_config.TextColumn(label="단위"),
            },
            disabled=["품목코드","품목명","단위","단가(원)"],
            hide_index=True, use_container_width=True, num_rows="fixed", height=380, key=editor_key,
        )
        st.markdown("</div>", unsafe_allow_html=True)

        # 일반 버튼(폼 X) → 클릭 1번에 바로 반영
        add_clicked = st.button("장바구니 추가", use_container_width=True, key="btn_cart_add")

    # 버튼 동작: 누적 담기(초기화 없음, 최신값 반영을 위해 세션에서 읽기)
    if add_clicked:
        cur = st.session_state.get(editor_key, edited_disp)
        if isinstance(cur, pd.DataFrame):
            tmp = cur[["품목코드","수량"]].copy()
            tmp["수량"] = pd.to_numeric(
                tmp["수량"].astype(str).str.replace(",", "").str.strip(),
                errors="coerce"
            ).fillna(0).astype(int)
            tmp = tmp[tmp["수량"] > 0]
            if tmp.empty:
                st.warning("수량이 0보다 큰 품목이 없습니다.")
            else:
                base = df_view[["품목코드","품목명","단위","단가"]].copy()
                base["단가"] = pd.to_numeric(base["단가"], errors="coerce").fillna(0).astype(int)
                tmp = tmp.merge(base, on="품목코드", how="left")[["품목코드","품목명","단위","단가","수량"]]
                _add_to_cart(tmp)                    # ✅ 누적 추가(초기화 없음)
                st.success("장바구니에 추가되었습니다.")
                st.session_state["store_editor_ver"] += 1  # 입력값 초기화
                st.rerun()

    # 3) 장바구니 (체크박스 + 오토세이브 + 회색 버튼 3개)
    with st.container(border=True):
        st.markdown("### 🧺 장바구니")

        cart = _coerce_price_qty(st.session_state["cart"]).copy()
        if not cart.empty:
            # 수량(TextColumn 렌더링용)
            cart["수량"] = pd.to_numeric(cart["수량"], errors="coerce").fillna(0).astype(int).astype(str)

            # 체크 상태 적용
            selected_set = set(map(str, st.session_state.get("cart_selected_codes", [])))
            cart_disp = cart.copy()
            cart_disp.insert(0, "선택", cart_disp["품목코드"].astype(str).isin(selected_set))

            cart_view = st.data_editor(
                cart_disp[["선택","품목코드","품목명","단위","수량","단가","총금액"]],
                column_config={
                    "선택":   st.column_config.CheckboxColumn(label=""),
                    "수량":   st.column_config.TextColumn(label="수량", help="숫자/콤마 입력 가능"),
                    "단가":   st.column_config.NumberColumn(label="단가(원)", min_value=0, step=1, format="%,d"),
                    "총금액": st.column_config.NumberColumn(label="총금액(원)", min_value=0, step=1, format="%,d"),
                    "품목코드": st.column_config.TextColumn(label="품목코드"),
                    "품목명":   st.column_config.TextColumn(label="품목명"),
                    "단위":     st.column_config.TextColumn(label="단위"),
                },
                disabled=["품목코드","품목명","단위","단가","총금액"],
                hide_index=True, use_container_width=True, height=340, key="cart_editor_live",
            )

            # 선택 상태 저장
            try:
                st.session_state["cart_selected_codes"] = (
                    cart_view.loc[cart_view["선택"] == True, "품목코드"].astype(str).tolist()  # noqa: E712
                )
            except Exception:
                st.session_state["cart_selected_codes"] = []

            # 오토세이브: 수량 정규화 + 합계 재계산
            updated = cart_view.drop(columns=["선택"], errors="ignore").copy()
            if "수량" in updated.columns:
                updated["수량"] = pd.to_numeric(
                    updated["수량"].astype(str).str.replace(",", "").str.strip(),
                    errors="coerce"
                ).fillna(0).astype(int)
            st.session_state["cart"] = _coerce_price_qty(updated)

            # 회색 버튼 3개
            st.markdown("<div class='muted-buttons'>", unsafe_allow_html=True)
            c1, c2, c3 = st.columns([1,1,1])

            all_codes = st.session_state["cart"]["품목코드"].astype(str).tolist()
            already_all = set(st.session_state.get("cart_selected_codes", [])) == set(all_codes) and len(all_codes) > 0
            toggle_label = "전체 해제" if already_all else "전체 선택"

            with c1:
                if st.button(toggle_label, use_container_width=True, key="btn_cart_toggle_all"):
                    st.session_state["cart_selected_codes"] = [] if already_all else all_codes
                    st.rerun()

            with c2:
                if st.button("선택 삭제", use_container_width=True, key="btn_cart_delete_selected"):
                    _remove_from_cart(st.session_state.get("cart_selected_codes", []))
                    st.session_state["cart_selected_codes"] = []
                    st.rerun()

            with c3:
                if st.button("장바구니 비우기", use_container_width=True, key="btn_cart_clear"):
                    _clear_cart()
                    st.session_state["cart_selected_codes"] = []
                    st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info("장바구니가 비어 있습니다.")

    # 하단 합계 바
    cart_now = _coerce_price_qty(st.session_state["cart"])
    total_items = len(cart_now)
    total_qty   = int(cart_now["수량"].sum())   if not cart_now.empty else 0
    total_amt   = int(cart_now["총금액"].sum()) if not cart_now.empty else 0
    req_date_str = 납품요청일.strftime("%Y-%m-%d")

    st.markdown(f"""
    <div class="sticky-bottom">
      <div>납품 요청일: <b>{req_date_str}</b></div>
      <div>선택 품목수: <span class="metric">{total_items:,}</span> 개</div>
      <div>총 수량: <span class="metric">{total_qty:,}</span></div>
      <div>총 금액: <span class="metric">{total_amt:,}</span> 원</div>
    </div>
    """, unsafe_allow_html=True)

    # 제출
    confirm = st.checkbox("제출 전 입력 내용 확인했습니다.", value=False, key="store_confirm_chk")
    if st.button("📦 발주 제출", type="primary", use_container_width=True, key="store_submit_btn"):
        if total_items == 0:
            st.warning("장바구니가 비어 있습니다."); st.stop()
        if not confirm:
            st.warning("체크박스를 확인해 주세요."); st.stop()

        user = st.session_state["auth"]
        order_id = make_order_id(user.get("user_id", "STORE"))
        now = now_kst_str()

        rows = []
        for _, r in cart_now.iterrows():
            rows.append({
                "주문일시": now, "발주번호": order_id,
                "지점ID": user.get("user_id"), "지점명": user.get("name"),
                "납품요청일": req_date_str,
                "품목코드": r.get("품목코드"), "품목명": r.get("품목명"),
                "단위": r.get("단위"),
                "수량": int(r.get("수량", 0) or 0),
                "단가": int(r.get("단가", 0) or 0),
                "금액": int((r.get("단가", 0) or 0) * (r.get("수량", 0) or 0)),
                "비고": memo or "", "상태": "접수", "처리일시": "", "처리자": ""
            })
        if append_orders(rows):
            st.success(f"발주가 접수되었습니다. 발주번호: {order_id}")
            _clear_cart(); st.session_state["cart_selected_codes"] = []
        else:
            st.error("발주 저장에 실패했습니다.")

# =============================================================================
# 8) 발주 조회·변경 — 장바구니형 선택/삭제 메커니즘 통일
# =============================================================================
def page_store_orders_change():
    st.subheader("🧾 발주 조회 · 변경")
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

    df_all = load_orders_df().copy()
    user = st.session_state["auth"]
    if df_all.empty:
        st.info("발주 데이터가 없습니다.")
        return
    df_all = df_all[df_all["지점ID"].astype(str) == user.get("user_id")]

    # 1) 조회 조건
    with st.container(border=True):
        st.markdown("### 🔎 조회 조건")
        c1, c2 = st.columns(2)
        with c1:
            dt_from = st.date_input("시작일", value=date.today() - timedelta(days=14), key="store_edit_from")
        with c2:
            dt_to = st.date_input("종료일", value=date.today(), key="store_edit_to")

    dt_series = pd.to_datetime(df_all["주문일시"], errors="coerce")
    mask = dt_series.notna() & (dt_series.dt.date >= dt_from) & (dt_series.dt.date <= dt_to)
    df = df_all[mask].copy().sort_values(["발주번호", "품목코드"])

    if df.empty:
        st.info("해당 기간에 조회할 발주가 없습니다.")
        return

    # 집계
    orders = df.groupby("발주번호").agg(
        건수=("품목코드", "count"),
        총수량=("수량", lambda x: pd.to_numeric(x, errors="coerce").fillna(0).sum()),
        총금액=("금액", lambda x: pd.to_numeric(x, errors="coerce").fillna(0).sum()),
        상태=("상태", lambda s: "출고완료" if (s == "출고완료").all() else "접수")
    ).reset_index()

    orders_pending = orders[orders["상태"] == "접수"].copy()
    orders_done    = orders[orders["상태"] == "출고완료"].copy()

    st.session_state.setdefault("orders_selected_ids", [])

    # 2) 발주 리스트
    with st.container(border=True):
        st.markdown("### 📦 발주 리스트")

        # (A) 접수 목록 — 체크 가능(장바구니와 동일한 회색 버튼)
        st.markdown("**접수(수정/삭제 가능)**")
        if not orders_pending.empty:
            selset = set(map(str, st.session_state.get("orders_selected_ids", [])))
            pend_disp = orders_pending.copy()
            pend_disp.insert(0, "선택", pend_disp["발주번호"].astype(str).isin(selset))

            pend_view = st.data_editor(
                pend_disp[["선택","발주번호","건수","총수량","총금액","상태"]],
                column_config={
                    "선택":   st.column_config.CheckboxColumn(label=""),
                    "총수량": st.column_config.NumberColumn(label="총 수량", min_value=0, step=1, format="%,d"),
                    "총금액": st.column_config.NumberColumn(label="총 금액", min_value=0, step=1, format="%,d"),
                },
                disabled=["발주번호","건수","총수량","총금액","상태"],
                use_container_width=True, height=240, hide_index=True, key="store_orders_list_pending"
            )
            try:
                st.session_state["orders_selected_ids"] = (
                    pend_view.loc[pend_view["선택"] == True, "발주번호"].astype(str).tolist()  # noqa: E712
                )
            except Exception:
                st.session_state["orders_selected_ids"] = []

            st.markdown("<div class='muted-buttons'>", unsafe_allow_html=True)
            c1, c2 = st.columns([1,1])

            all_ids = orders_pending["발주번호"].astype(str).tolist()
            already_all = set(st.session_state.get("orders_selected_ids", [])) == set(all_ids) and len(all_ids) > 0
            toggle_label = "전체 해제" if already_all else "전체 선택"

            with c1:
                if st.button(toggle_label, use_container_width=True, key="btn_orders_toggle_all"):
                    st.session_state["orders_selected_ids"] = [] if already_all else all_ids
                    st.rerun()

            with c2:
                if st.button("선택 발주 삭제", use_container_width=True, key="btn_orders_delete"):
                    ids = st.session_state.get("orders_selected_ids", [])
                    if ids:
                        base = load_orders_df().copy()
                        # 접수 건만 삭제
                        del_mask = base["발주번호"].astype(str).isin(ids) & (base["상태"] != "출고완료")
                        keep = base[~del_mask].copy()
                        ok = write_orders_df(keep)
                        if ok:
                            st.success("선택한 발주(접수)를 삭제했습니다.")
                            st.session_state["orders_selected_ids"] = []
                            st.rerun()
                        else:
                            st.error("삭제 실패")
                    else:
                        st.info("삭제할 발주가 선택되지 않았습니다.")
            st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info("접수 상태의 발주가 없습니다.")

        st.markdown("---")

        # (B) 출고완료 목록 — 선택 불가(표시만)
        st.markdown("**출고완료(선택 불가)**")
        if not orders_done.empty:
            st.dataframe(
                orders_done[["발주번호","건수","총수량","총금액","상태"]],
                use_container_width=True, height=200,
                column_config={
                    "총수량": st.column_config.NumberColumn(label="총 수량", min_value=0, step=1, format="%,d"),
                    "총금액": st.column_config.NumberColumn(label="총 금액", min_value=0, step=1, format="%,d"),
                }
            )
        else:
            st.caption("출고완료 상태의 발주가 없습니다.")

    # 3) 세부 내용 확인 (발주번호 선택 시 품목 목록 + 합계)
    with st.container(border=True):
        st.markdown("### 📄 세부 내용 확인")
        options = orders["발주번호"].astype(str).tolist()
        target_order = st.radio("발주번호 선택", options=options, key="store_edit_pick")
        if not target_order:
            st.info("발주번호를 선택하세요.")
            return

        target_df = df[df["발주번호"].astype(str) == target_order].copy()
        if target_df.empty:
            st.info("해당 발주에 품목이 없습니다.")
            return

        # 타입 정규화
        target_df["수량"] = pd.to_numeric(target_df["수량"], errors="coerce").fillna(0).astype(int)
        target_df["단가"] = pd.to_numeric(target_df["단가"], errors="coerce").fillna(0).astype(int)
        target_df["금액"] = (target_df["수량"] * target_df["단가"]).astype(int)

        # 표(읽기 전용)
        show_cols = ["품목코드","품목명","단위","수량","단가","금액","비고"]
        st.dataframe(
            target_df[show_cols], use_container_width=True, height=380, hide_index=True,
            column_config={
                "수량": st.column_config.NumberColumn(label="수량", min_value=0, step=1, format="%,d"),
                "단가": st.column_config.NumberColumn(label="단가(원)", min_value=0, step=1, format="%,d"),
                "금액": st.column_config.NumberColumn(label="금액(원)", min_value=0, step=1, format="%,d"),
            }
        )

        # 합계 요약(등록 화면과 동일 포맷)
        total_items = len(target_df)  # 행 수(=선택 품목수)
        total_qty   = int(target_df["수량"].sum())
        total_amt   = int(target_df["금액"].sum())
        req_date    = str(target_df["납품요청일"].iloc[0]) if "납품요청일" in target_df.columns else "-"

        st.markdown(f"""
        <div class="card-tight" style="display:flex; gap:16px; align-items:center; justify-content:flex-start; margin-top:8px;">
            <div>납품 요청일: <b>{req_date}</b></div>
            <div>선택 품목수: <span class="metric">{total_items:,}</span> 개</div>
            <div>총 수량: <span class="metric">{total_qty:,}</span></div>
            <div>총 금액: <span class="metric">{total_amt:,}</span> 원</div>
        </div>
        """, unsafe_allow_html=True)

        st.caption(f"상태: {'출고완료' if (target_df['상태'] == '출고완료').all() else '접수'}  ·  발주번호: {target_order}")

# =============================================================================
# 9) 발주서 조회·다운로드
# =============================================================================
def page_store_order_form_download(master_df: pd.DataFrame):
    st.subheader("📑 발주서 조회 · 다운로드")
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

    df = load_orders_df().copy()
    if df.empty:
        st.info("발주 데이터가 없습니다.")
        return
    user = st.session_state["auth"]
    df = df[df["지점ID"].astype(str) == user.get("user_id")]

    with st.container(border=True):
        st.markdown("### 🔎 조회 조건")
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            dt_from = st.date_input("시작일", value=date.today() - timedelta(days=7), key="store_dl_from")
        with c2:
            dt_to = st.date_input("종료일", value=date.today(), key="store_dl_to")
        with c3:
            order_ids = ["(전체)"] + sorted(df["발주번호"].dropna().unique().tolist())
            target_order = st.selectbox("발주번호(선택 시 해당 건만)", order_ids, key="store_dl_orderid")

    dt_series = pd.to_datetime(df["주문일시"], errors="coerce")
    mask = dt_series.notna() & (dt_series.dt.date >= dt_from) & (dt_series.dt.date <= dt_to)
    if target_order != "(전체)":
        mask &= (df["발주번호"] == target_order)
    dfv = df[mask].copy().sort_values(["발주번호", "품목코드"])

    with st.container(border=True):
        st.markdown("### 📄 미리보기")
        st.dataframe(
            dfv, use_container_width=True, height=420,
            column_config={
                "단가": numcol("단가(원)"),
                "금액": numcol("총금액(원)"),
                "수량": numcol("수량"),
            }
        )

    period_text = f"{dt_from:%Y-%m-%d} ~ {dt_to:%Y-%m-%d}"
    buf = make_order_sheet_excel(dfv, include_price=False, title="산카쿠 납품내역서", period_text=period_text)
    fname = make_filename("산카쿠 납품내역서", dt_from, dt_to)
    st.download_button(
        "발주서 엑셀 다운로드", data=buf.getvalue(),
        file_name=fname,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="store_dl_btn"
    )

# =============================================================================
# 10) 발주 품목 가격 조회
# =============================================================================
def page_store_master_view(master_df: pd.DataFrame):
    st.subheader("🏷️ 발주 품목 가격 조회")
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

    cols = [c for c in ["품목코드", "품목명", "분류", "단위", "단가"] if c in master_df.columns]
    view = master_df[cols].copy()
    view["단가"] = pd.to_numeric(view.get("단가", 0), errors="coerce").fillna(0).astype(int)

    with st.container(border=True):
        st.markdown("### 📋 품목 리스트")
        st.dataframe(
            view, use_container_width=True, height=480,
            column_config={"단가": numcol("단가(원)")}
        )

# =============================================================================
# 관리자 화면
# =============================================================================
def page_admin_orders_manage(master_df: pd.DataFrame):
    st.subheader("🗂️ 주문 관리 · 출고확인")
    df = load_orders_df().copy()
    if df.empty:
        st.info("발주 데이터가 없습니다."); return

    with st.container(border=True):
        st.markdown("### 🔎 조회 조건")
        c1, c2, c3, c4 = st.columns([1,1,1,2])
        with c1: dt_from = st.date_input("시작일", value=date.today()-timedelta(days=3), key="admin_mng_from")
        with c2: dt_to   = st.date_input("종료일", value=date.today(), key="admin_mng_to")
        with c3:
            stores = ["(전체)"] + sorted(df["지점명"].dropna().unique().tolist())
            store = st.selectbox("지점", stores, key="admin_mng_store")
        with c4:
            status = st.multiselect("상태", ORDER_STATUSES, default=ORDER_STATUSES, key="admin_mng_status")

    dt_series = pd.to_datetime(df["주문일시"], errors="coerce")
    mask = dt_series.notna() & (dt_series.dt.date >= dt_from) & (dt_series.dt.date <= dt_to)
    if store != "(전체)": mask &= (df["지점명"]==store)
    if status: mask &= df["상태"].isin(status)
    dfv = df[mask].copy().sort_values(["발주번호","품목코드"])

    with st.container(border=True):
        st.markdown("### 📦 조회 결과")
        st.caption(f"조회 건수: {len(dfv):,}건")
        st.dataframe(dfv, use_container_width=True, height=420)
        st.download_button("CSV 다운로드",
                           data=dfv.to_csv(index=False).encode("utf-8-sig"),
                           file_name="orders_admin.csv",
                           mime="text/csv",
                           key="admin_mng_csv")

    with st.container(border=True):
        st.markdown("### ✅ 출고 처리 (이미 출고완료된 발주번호는 제외)")
        if not dfv.empty:
            candidates = sorted(dfv[dfv["상태"]=="접수"]["발주번호"].dropna().unique().tolist())
            sel_ids = st.multiselect("발주번호 선택", candidates, key="admin_mng_pick_ids")
            if st.button("선택 발주 출고완료 처리", type="primary", key="admin_mng_ship_btn"):
                if sel_ids:
                    ok = update_order_status(sel_ids, new_status="출고완료",
                                             handler=st.session_state["auth"].get("name","관리자"))
                    if ok: st.success("출고완료 처리되었습니다.")
                    else: st.error("상태 변경 실패")
                else:
                    st.warning("발주번호를 선택하세요.")

def page_admin_shipments_change():
    st.subheader("🚚 출고내역 조회 · 상태변경")
    df = load_orders_df().copy()
    if df.empty:
        st.info("발주 데이터가 없습니다."); return

    with st.container(border=True):
        st.markdown("### 🔎 조회 조건")
        c1, c2 = st.columns(2)
        with c1: dt_from = st.date_input("시작일", value=date.today()-timedelta(days=7), key="admin_ship_from")
        with c2: dt_to   = st.date_input("종료일", value=date.today(), key="admin_ship_to")

    dt_series = pd.to_datetime(df["주문일시"], errors="coerce")
    mask = dt_series.notna() & (dt_series.dt.date >= dt_from) & (dt_series.dt.date <= dt_to)
    dfv = df[mask].copy().sort_values(["발주번호","품목코드"])

    with st.container(border=True):
        st.markdown("### 📦 조회 결과")
        st.caption(f"조회 건수: {len(dfv):,}건")
        orders = dfv.groupby("발주번호").agg(건수=("품목코드","count"),
                                          상태=("상태", lambda s: "출고완료" if (s=="출고완료").all() else "접수")).reset_index()
        st.dataframe(orders, use_container_width=True, height=220)

    with st.container(border=True):
        st.markdown("### 📝 출고 상태 일괄 변경 (발주번호 단위)")
        order_ids = sorted(dfv["발주번호"].dropna().unique().tolist())
        target = st.multiselect("발주번호", order_ids, key="admin_ship_change_ids")
        new_status = st.selectbox("새 상태", ORDER_STATUSES, index=0, key="admin_ship_new_status")
        if st.button("상태 변경 저장", type="primary", key="admin_ship_save"):
            if not target: st.warning("발주번호를 선택하세요."); return
            ok = update_order_status(target, new_status=new_status,
                                     handler=st.session_state["auth"].get("name","관리자"))
            if ok: st.success("상태 변경 완료")
            else: st.error("상태 변경 실패")

def page_admin_delivery_note(master_df: pd.DataFrame):
    st.subheader("📑 출고 내역서 조회 · 다운로드")
    df = load_orders_df().copy()
    if df.empty:
        st.info("발주 데이터가 없습니다."); return

    with st.container(border=True):
        st.markdown("### 🔎 조회 조건")
        c0, c1, c2, c3 = st.columns([1,1,1,2])
        with c0:
            quick = st.radio("기간", ["오늘","직접선택"], horizontal=True, key="admin_note_quick")
        with c1:
            dt_from = date.today() if quick=="오늘" else st.date_input("시작일", value=date.today()-timedelta(days=7), key="admin_note_from")
        with c2:
            dt_to   = date.today() if quick=="오늘" else st.date_input("종료일", value=date.today(), key="admin_note_to")
        with c3:
            stores = ["(전체)"] + sorted(df["지점명"].dropna().unique().tolist())
            store  = st.selectbox("지점(선택)", stores, key="admin_note_store")

    dt_series = pd.to_datetime(df["주문일시"], errors="coerce")
    mask = dt_series.notna() & (dt_series.dt.date >= dt_from) & (dt_series.dt.date <= dt_to)
    if store != "(전체)":
        mask &= (df["지점명"]==store)
    dfv = df[mask].copy().sort_values(["발주번호","품목코드"])

    with st.container(border=True):
        st.markdown("### 📄 미리보기")
        st.dataframe(dfv, use_container_width=True, height=420)

    period_text = f"{dt_from:%Y-%m-%d} ~ {dt_to:%Y-%m-%d}" + ("" if store=="(전체)" else f" | 지점: {store}")
    buf = make_order_sheet_excel(dfv, include_price=True, title="산카쿠 납품내역서", period_text=period_text)
    fname = make_filename("산카쿠 납품내역서", dt_from, dt_to)
    st.download_button("출고 내역서 엑셀 다운로드", data=buf.getvalue(),
                       file_name=fname,
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       key="admin_note_btn")

def page_admin_items_price(master_df: pd.DataFrame):
    st.subheader("🏷️ 납품 품목 가격 설정")
    cols = [c for c in ["품목코드","품목명","분류","단위","단가","활성"] if c in master_df.columns]
    view = master_df[cols].copy()
    view["삭제"] = False
    st.caption("단가·활성(선택)을 수정하거나 삭제 체크 후 [변경사항 저장]을 누르면 상품마스터 시트에 반영됩니다.")

    with st.container(border=True):
        st.markdown("### ✏️ 가격/활성 편집")
        edited = st.data_editor(
            view, use_container_width=True, hide_index=True, num_rows="dynamic",
            column_config={
                "단가": numcol("단위당 단가"),
                "활성": st.column_config.CheckboxColumn(),
                "삭제": st.column_config.CheckboxColumn()
            },
            key="admin_master_editor"
        )
        if st.button("변경사항 저장", type="primary", key="admin_master_save"):
            if isinstance(edited, pd.DataFrame):
                edited = edited[~edited["삭제"].fillna(False)].drop(columns=["삭제"])
            if "단가" in edited.columns:
                edited["단가"] = pd.to_numeric(edited["단가"], errors="coerce").fillna(0).astype(int)
            ok = write_master_df(edited)
            if ok:
                st.success("상품마스터에 저장되었습니다.")
                st.cache_data.clear()
            else:
                st.error("저장 실패")

# =============================================================================
# 9) 라우팅
# =============================================================================
if __name__ == "__main__":
    if not require_login():
        st.stop()

    st.title("📦 식자재 발주 시스템")

    user = st.session_state["auth"]
    role = user.get("role", "store")
    master = load_master_df()

    if role == "admin":
        t1, t2, t3, t4 = st.tabs(["주문 관리·출고확인", "출고내역 조회·상태변경", "출고 내역서 다운로드", "납품 품목 가격 설정"])
        with t1: page_admin_orders_manage(master)
        with t2: page_admin_shipments_change()
        with t3: page_admin_delivery_note(master)
        with t4: page_admin_items_price(master)
    else:
        # ▶ 네이밍 통일: '발주 요청'
        t1, t2, t3, t4 = st.tabs(["발주 요청", "발주 조회·변경", "발주서 다운로드", "발주 품목 가격 조회"])
        with t1: page_store_register_confirm(master)
        with t2: page_store_orders_change()
        with t3: page_store_order_form_download(master)
        with t4: page_store_master_view(master)
