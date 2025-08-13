# -*- coding: utf-8 -*-
# =============================================================================
# 📦 Streamlit 식자재 발주 시스템 (KST/발주번호·UX·엑셀서식/관리자 개선 통합판)
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
# (requirements: streamlit, pandas, gspread, google-auth, gspread-dataframe, openpyxl, xlrd, xlsxwriter)
import xlsxwriter  # noqa: F401 (엔진 로딩용)

# -----------------------------------------------------------------------------
# 페이지/테마/스타일 (최소 수정판 - 오류 수정)
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
/* =========================
   Global
========================= */
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

/* 본문을 더 좁게(양옆 여백 ↑) */
[data-testid="stAppViewContainer"] .main .block-container {{
  max-width: 1050px;     /* 980~1100으로 취향대로 조정 가능 */
  margin: 0 auto;
  padding-left: 12px;
  padding-right: 12px;
}}

/* =========================
   Inputs / Tables
========================= */
.stButton>button {{
  background:{THEME['PRIMARY']};
  color:#fff;
  border:1px solid {THEME['PRIMARY']};
  border-radius:10px;
  height:34px;
}}
.stButton>button:hover {{ filter: brightness(0.95); }}

.stTextInput>div>div>input,
.stNumberInput input,
.stDateInput input {{
  border:1px solid {THEME['BORDER']} !important;
  border-radius:10px !important;
  height:34px;
}}

.dataframe, .stDataFrame, .stTable {{
  background:{THEME['CARD_BG']};
  border-radius:12px;
  border:1px solid {THEME['BORDER']};
}}
.dataframe td, .dataframe th {{ vertical-align: middle; }}

/* =========================
   Tabs: 카드형 + 간격 확장
   (신규 DOM: button[role="tab"] 대응)
========================= */
/* 탭 컨테이너 */
.stTabs [role="tablist"],
div[role="tablist"] {{
  display: flex !important;
  gap: 12px !important;          /* 탭 사이 간격 */
  flex-wrap: wrap !important;    /* 좁을 때 줄바꿈 */
  margin-top: 8px !important;
  margin-bottom: 24px !important;/* 탭과 본문 사이 여백 */
  border-bottom: none !important;
}}
/* 탭 버튼을 카드처럼 */
.stTabs button[role="tab"],
button[role="tab"] {{
  border: 1px solid {THEME['BORDER']} !important;
  border-radius: 12px !important;
  background: #fff !important;
  padding: 10px 14px !important; /* 클릭 면적 ↑ */
  box-shadow: 0 1px 6px rgba(0,0,0,0.04) !important;
  cursor: pointer !important;
  transition: transform .08s ease, box-shadow .12s ease, border-color .12s ease, background-color .12s ease;
}}
/* 호버 효과 */
.stTabs button[role="tab"]:hover,
button[role="tab"]:hover {{
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(0,0,0,0.08);
}}
/* 선택된 탭 강조 */
.stTabs button[role="tab"][aria-selected="true"],
button[role="tab"][aria-selected="true"] {{
  border-color: {THEME['PRIMARY']} !important;
  color: {THEME['PRIMARY']} !important;
  box-shadow: 0 6px 16px rgba(28,103,88,0.18) !important;
  background: #ffffff !important;
  font-weight: 700;
}}
.stTabs {{ overflow: visible !important; }}
.stTabs [role="tablist"] {{ position: relative; z-index: 3; }}

/* 구버전 하이라이트 바 제거(양쪽 DOM 모두 커버) */
.stTabs [data-baseweb="tab-highlight"],
[data-baseweb="tab-highlight"] {{ display: none !important; }}

/* =========================
   Sticky summary
========================= */
.sticky-bottom {{
  position: sticky; bottom: 0; z-index: 999;
  {CARD_STYLE}
  margin-top:10px; display:flex; align-items:center; justify-content:space-between; gap:16px;
}}

/* =========================
   로그인 / 타이틀
========================= */
.login-wrap {{ display:flex; justify-content:center; }}
.login-title {{
  text-align: center;
  font-size: 42px;
  font-weight: 800;
  margin-top: 16px;
  margin-bottom: 12px;
}}
.login-card {{
  width: 300px;
  margin-top: 16px; padding: 16px;
  border:1px solid {THEME['BORDER']};
  border-radius:12px; background:#fff; box-shadow: 0 4px 12px rgba(0,0,0,.04);
}}
.login-card .stTextInput>div>div>input {{ width: 220px; height: 32px; }}
.login-card .stButton>button {{ width: 220px; height: 32px; }}

.page-title {{
  font-size: 34px;
  font-weight: 800;
  margin-top: 12px;
  margin-bottom: 12px;
}}

.tabs-spacer {{ height: 10px; }}


</style>
""", unsafe_allow_html=True)

# --- 공용 작은 UI 유틸(그대로 유지) ---
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
# 1) Users 로더 (여러 시크릿 포맷 지원)
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
    # 활성 필터
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
    # 스키마 보정
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
# 5) 로그인 (아이디 또는 지점명) + verify_password
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

    # ⬇️ 로그인 화면 상단 여백(뷰포트 기준) — 잘림 방지
    st.markdown("<div style='height:8vh'></div>", unsafe_allow_html=True)

    # 제목(글자 크게, 아래에 약간 공간)
    st.markdown('<div class="login-title">식자재 발주 시스템</div>', unsafe_allow_html=True)
    st.markdown("<div class='tabs-spacer'></div>", unsafe_allow_html=True)  
    # 제목과 폼 사이도 살짝 띄우기
    st.markdown("<div style='height:1vh'></div>", unsafe_allow_html=True)

    # 가운데 좁은 컬럼에 폼 배치 → 위젯 폭 과다 방지
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
    # 포맷: YYYYMMDDHHMM + 지점ID (예: 202508022055jeondae)
    return f"{datetime.now(KST):%Y%m%d%H%M}{store_id}"

def make_order_sheet_excel(df_note: pd.DataFrame, include_price: bool, *,
                           title: str = "산카쿠 납품내역서",
                           period_text: Optional[str] = None) -> BytesIO:
    """
    발주/출고 내역 엑셀 생성 (KST, 머리표 포함, NaN 안전)
    """
    buf = BytesIO()

    # 내보낼 컬럼 구성
    cols = ["발주번호","주문일시","납품요청일","지점명","품목코드","품목명","단위","수량","비고","상태"]
    if include_price:
        for c in ["단가","금액"]:
            if c not in df_note.columns:
                df_note[c] = 0
        cols += ["단가","금액"]

    export = df_note[cols].copy().sort_values(["발주번호","품목코드"])

    # 숫자 보정
    export["수량"] = pd.to_numeric(export.get("수량", 0), errors="coerce").fillna(0)
    if include_price:
        export["단가"] = pd.to_numeric(export.get("단가", 0), errors="coerce").fillna(0)
        export["금액"] = pd.to_numeric(export.get("금액", 0), errors="coerce").fillna(0)

    # UI 표기용: 단가 헤더명 변경
    col_map = {}
    if include_price and "단가" in export.columns:
        col_map["단가"] = "단위당 단가"
    export = export.rename(columns=col_map)

    startrow = 4
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        export.to_excel(w, index=False, sheet_name="내역", startrow=startrow)
        wb = w.book
        ws = w.sheets["내역"]

        # 서식
        fmt_title = wb.add_format({"bold": True, "font_size": 16, "align":"center", "valign":"vcenter"})
        fmt_info  = wb.add_format({"font_size": 10})
        fmt_th    = wb.add_format({"bold": True, "bg_color":"#F2F2F2", "border":1})
        fmt_n     = wb.add_format({"num_format":"#,##0"})
        fmt_txt   = wb.add_format({})
        fmt_sum_l = wb.add_format({"bold": True})
        fmt_sum_n = wb.add_format({"bold": True, "num_format":"#,##0"})

        # 제목/기간/생성일시
        ncols = len(export.columns)
        ws.merge_range(0, 0, 0, ncols-1, title, fmt_title)
        ws.write(1, 0, f"조회기간: {period_text or ''}", fmt_info)
        ws.write(2, 0, f"생성일시(KST): {now_kst_str()}", fmt_info)

        # 헤더 서식
        for c in range(ncols):
            ws.write(startrow, c, export.columns[c], fmt_th)

        # 숫자열 서식 적용
        def col_idx(col_name: str) -> Optional[int]:
            try:
                return export.columns.get_loc(col_name)
            except Exception:
                return None

        idx_qty = col_idx("수량")
        if idx_qty is not None:
            ws.set_column(idx_qty, idx_qty, 10, fmt_n)
        if include_price:
            idx_unit = col_idx("단위당 단가")
            idx_amt  = col_idx("금액")
            if idx_unit is not None: ws.set_column(idx_unit, idx_unit, 12, fmt_n)
            if idx_amt  is not None: ws.set_column(idx_amt, idx_amt, 14, fmt_n)

        # 기타 컬럼 너비
        auto_w = {"발주번호":16, "주문일시":19, "납품요청일":12, "지점명":12, "품목코드":10, "품목명":18, "단위":8, "비고":18, "상태":10}
        for k, wth in auto_w.items():
            i = col_idx(k)
            if i is not None:
                ws.set_column(i, i, wth, fmt_txt)

        # 합계
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

# ──────────────────────────────────────────────
# 🛒 발주(지점) 화면 — 누적 장바구니 + 단가 안정화 + 장바구니 수량 직접수정
# ──────────────────────────────────────────────

# ── 장바구니 유틸 ──────────────────────────────
def _ensure_cart():
    if "cart" not in st.session_state or not isinstance(st.session_state["cart"], pd.DataFrame):
        st.session_state["cart"] = pd.DataFrame(columns=["품목코드","품목명","단위","단가","수량","총금액"])

def _coerce_price_qty(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["단가"] = pd.to_numeric(out.get("단가", 0), errors="coerce").fillna(0).astype(int)
    out["수량"] = pd.to_numeric(out.get("수량", 0), errors="coerce").fillna(0).astype(int)
    out["총금액"] = (out["단가"] * out["수량"]).astype(int)
    return out

def _add_to_cart(rows_df: pd.DataFrame):
    _ensure_cart()
    cart = _coerce_price_qty(st.session_state["cart"])
    add  = _coerce_price_qty(rows_df[["품목코드","품목명","단위","단가","수량"]].copy())
    add = add[add["수량"] > 0]
    if add.empty:
        return
    key = ["품목코드"]
    merged = pd.merge(cart.drop(columns=["총금액"], errors="ignore"), add, on=key, how="outer", suffixes=("_old",""))
    merged["품목명"] = merged["품목명"].fillna(merged.get("품목명_old"))
    merged["단위"]   = merged["단위"].fillna(merged.get("단위_old"))
    merged["단가"]   = merged["단가"].fillna(merged.get("단가_old")).fillna(0).astype(int)
    qty_old = pd.to_numeric(merged.get("수량_old", 0), errors="coerce").fillna(0).astype(int)
    qty_new = pd.to_numeric(merged.get("수량",     0), errors="coerce").fillna(0).astype(int)
    merged["수량"] = (qty_old + qty_new).astype(int)
    for c in ["품목명_old","단위_old","단가_old","수량_old"]:
        if c in merged.columns:
            merged.drop(columns=[c], inplace=True)
    merged = merged[merged["수량"] > 0]
    merged["총금액"] = (merged["단가"] * merged["수량"]).astype(int)
    st.session_state["cart"] = merged[["품목코드","품목명","단위","단가","수량","총금액"]]

def _remove_from_cart(codes: list[str]):
    _ensure_cart()
    if not codes:
        return
    st.session_state["cart"] = st.session_state["cart"][~st.session_state["cart"]["품목코드"].isin(codes)]

def _clear_cart():
    st.session_state["cart"] = pd.DataFrame(columns=["품목코드","품목명","단위","단가","수량","총금액"])

# ── 발주 화면 본체 ─────────────────────────────
def page_store_register_confirm(master_df: pd.DataFrame):
    _ensure_cart()

    st.subheader("🛒 발주 등록 · 확인")
    st.markdown("<div class='center-narrow'>", unsafe_allow_html=True)

    # ── 납품 선택 ─────────────────────────────
    st.markdown("<div class='section'><div class='box'>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        quick = st.radio("납품 선택", ["오늘", "내일", "직접선택"], horizontal=True, key="store_quick_radio")
    with c2:
        납품요청일 = (
            date.today() if quick == "오늘" else
            (date.today() + timedelta(days=1) if quick == "내일" else
             st.date_input("납품 요청일", value=date.today(), key="store_req_date"))
        )
    with c3:
        memo = st.text_input("요청 사항(선택)", key="store_req_memo")
    st.markdown("</div></div>", unsafe_allow_html=True)

    # ── 발주 품목 검색 ────────────────────────
    st.markdown("<div class='section'><div class='box'>", unsafe_allow_html=True)
    st.markdown("### 1) 발주 품목 검색")
    l, r = st.columns([2, 1])
    df_master = master_df.copy()
    df_master["단가"] = pd.to_numeric(df_master.get("단가", 0), errors="coerce").fillna(0).astype(int)

    with l:
        keyword = st.text_input("품목 검색(이름/코드)", key="store_kw")
    with r:
        if "분류" in df_master.columns:
            cat_opt = ["(전체)"] + sorted(df_master["분류"].dropna().unique().tolist())
            cat_sel = st.selectbox("분류(선택)", cat_opt, key="store_cat_sel")
        else:
            cat_sel = "(전체)"

    df_view = df_master
    if keyword:
        q = keyword.strip().lower()
        df_view = df_view[df_view.apply(
            lambda row: q in str(row.get("품목명", "")).lower()
                        or q in str(row.get("품목코드", "")).lower(),
            axis=1
        )]
    if "분류" in df_master.columns and cat_sel != "(전체)":
        df_view = df_view[df_view["분류"] == cat_sel]

    df_preview = df_view.copy()
    df_preview["단가(원)"] = df_preview["단가"].map(lambda v: f"{v:,.0f}")
    cols_preview = [c for c in ["품목코드", "품목명", "분류", "단위", "단가(원)"] if c in df_preview.columns]
    st.dataframe(df_preview[cols_preview].reset_index(drop=True), use_container_width=True, height=260)
    st.markdown("</div></div>", unsafe_allow_html=True)

    # ── 발주 수량 입력 ────────────────────────
    st.markdown("<div class='section'><div class='box'>", unsafe_allow_html=True)
    st.markdown("### 2) 발주 수량 입력")
    df_edit = df_view[["품목코드", "품목명", "단위", "단가"]].copy()
    df_edit["수량"] = 0
    df_edit = _coerce_price_qty(df_edit)

    with st.form(key="store_order_form", clear_on_submit=False):
        edited = st.data_editor(
            df_edit,
            column_config={
                "단가":  st.column_config.NumberColumn(label="단가(원)", format="%,d", step=1),
                "수량":  st.column_config.NumberColumn(label="수량", min_value=0, step=1),
                "품목코드": st.column_config.TextColumn(label="품목코드"),
                "품목명": st.column_config.TextColumn(label="품목명"),
                "단위":   st.column_config.TextColumn(label="단위"),
            },
            disabled=["품목코드", "품목명", "단위", "단가"],  # 수량만 입력
            hide_index=True,
            use_container_width=True,
            num_rows="fixed",
            height=360,
            key="store_order_editor",
        )
        col_btn1, col_btn2 = st.columns([1,1])
        with col_btn1:
            submitted_add = st.form_submit_button("장바구니 반영", use_container_width=True)
        with col_btn2:
            submitted_add_clear = st.form_submit_button("장바구니 반영 후 입력값 초기화", use_container_width=True)

    if isinstance(edited, pd.DataFrame) and (submitted_add or submitted_add_clear):
        tmp = _coerce_price_qty(edited.copy())
        tmp = tmp[tmp["수량"] > 0]
        if tmp.empty:
            st.warning("수량이 0보다 큰 품목이 없습니다.")
        else:
            _add_to_cart(tmp)
            st.success("장바구니에 반영되었습니다.")
            if submitted_add_clear:
                st.session_state["store_order_editor"] = df_edit

    st.markdown("</div></div>", unsafe_allow_html=True)

    # ── 장바구니 (수량 직접 수정 가능) ───────────────────────────
    st.markdown("<div class='section'><div class='box'>", unsafe_allow_html=True)
    st.markdown("### 3) 발주 입력 내역 (장바구니)")

    cart = _coerce_price_qty(st.session_state["cart"])
    if not cart.empty:
        # 편집 가능한 장바구니
        with st.form(key="cart_edit_form", clear_on_submit=False):
            cart_editable = st.data_editor(
                cart[["품목코드","품목명","단위","수량","단가","총금액"]],
                column_config={
                    "수량":   st.column_config.NumberColumn(label="수량", min_value=0, step=1, format="%,d"),
                    "단가":   st.column_config.NumberColumn(label="단가(원)", format="%,d", step=1),
                    "총금액": st.column_config.NumberColumn(label="총금액(원)", format="%,d"),
                    "품목코드": st.column_config.TextColumn(label="품목코드"),
                    "품목명": st.column_config.TextColumn(label="품목명"),
                    "단위":   st.column_config.TextColumn(label="단위"),
                },
                disabled=["품목코드","품목명","단위","단가","총금액"],  # 장바구니에서는 수량만 수정
                hide_index=True,
                use_container_width=True,
                height=300,
                key="cart_editor",
            )
            c_upd1, c_upd2 = st.columns([1,1])
            with c_upd1:
                save_cart = st.form_submit_button("장바구니 변경 저장", use_container_width=True)
            with c_upd2:
                cancel_cart = st.form_submit_button("변경 취소(새로고침)", use_container_width=True)

        if save_cart and isinstance(cart_editable, pd.DataFrame):
            # 수량 반영 및 총금액 재계산, 0인 품목 제거
            updated = _coerce_price_qty(cart_editable.copy())
            updated = updated[updated["수량"] > 0]
            st.session_state["cart"] = updated[["품목코드","품목명","단위","단가","수량","총금액"]]
            st.success("장바구니 변경사항이 저장되었습니다.")
            st.rerun()
        elif cancel_cart:
            st.rerun()

        # 합계 계산(저장된 장바구니 기준)
        cart = _coerce_price_qty(st.session_state["cart"])
        total_items = len(cart)
        total_qty   = int(cart["수량"].sum())
        total_amt   = int(cart["총금액"].sum())

        # 선택 삭제/비우기
        st.markdown("##### 선택 삭제")
        to_delete = st.multiselect(
            "삭제할 품목코드 선택",
            options=cart["품목코드"].tolist(),
            format_func=lambda x: f"{x} — {cart.loc[cart['품목코드']==x, '품목명'].values[0]}"
        )
        cdel1, cdel2 = st.columns([1,1])
        with cdel1:
            if st.button("선택 품목 삭제", use_container_width=True):
                _remove_from_cart(to_delete)
                st.rerun()
        with cdel2:
            if st.button("장바구니 비우기", use_container_width=True):
                _clear_cart()
                st.rerun()
    else:
        total_items = total_qty = total_amt = 0
        st.info("장바구니가 비어 있습니다.")

    # 합계 바
    st.markdown(f"""
    <div class="sticky-bottom">
      <div>납품 요청일: <b>{납품요청일.strftime('%Y-%m-%d')}</b></div>
      <div>선택 품목수: <span class="metric">{total_items:,}</span> 개</div>
      <div>총 수량: <span class="metric">{total_qty:,}</span></div>
      <div>총 금액: <span class="metric">{total_amt:,}</span> 원</div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("</div></div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

    # ── 제출 ───────────────────────────────
    confirm = st.checkbox("제출 전 입력 내용 확인했습니다.", value=False, key="store_confirm_chk")
    if st.button("📦 발주 제출", type="primary", use_container_width=True, key="store_submit_btn"):
        if total_items == 0:
            st.warning("장바구니가 비어 있습니다.")
            st.stop()
        if not confirm:
            st.warning("체크박스를 확인해 주세요.")
            st.stop()

        user = st.session_state["auth"]
        order_id = make_order_id(user.get("user_id", "STORE"))
        now = now_kst_str()

        rows = []
        cart_final = _coerce_price_qty(st.session_state["cart"])
        for _, r in cart_final.iterrows():
            rows.append({
                "주문일시": now, "발주번호": order_id,
                "지점ID": user.get("user_id"), "지점명": user.get("name"),
                "납품요청일": str(납품요청일),
                "품목코드": r.get("품목코드"), "품목명": r.get("품목명"),
                "단위": r.get("단위"),
                "수량": int(r.get("수량", 0) or 0),
                "단가": int(r.get("단가", 0) or 0),
                "금액": int((r.get("단가", 0) or 0) * (r.get("수량", 0) or 0)),
                "비고": memo or "", "상태": "접수", "처리일시": "", "처리자": ""
            })

        if append_orders(rows):
            st.success(f"발주가 접수되었습니다. 발주번호: {order_id}")
            _clear_cart()
        else:
            st.error("발주 저장에 실패했습니다.")


# =============================================================================
# 8) 발주 조회·변경 — 정리본
# =============================================================================
def page_store_orders_change():
    st.subheader("🧾 발주 조회 · 변경")
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

    df = load_orders_df().copy()
    user = st.session_state["auth"]
    if df.empty:
        st.info("발주 데이터가 없습니다.")
        return
    df = df[df["지점ID"].astype(str) == user.get("user_id")]

    c1, c2 = st.columns(2)
    with c1:
        dt_from = st.date_input("시작일", value=date.today() - timedelta(days=14), key="store_edit_from")
    with c2:
        dt_to = st.date_input("종료일", value=date.today(), key="store_edit_to")

    dt_series = pd.to_datetime(df["주문일시"], errors="coerce")
    mask = dt_series.notna() & (dt_series.dt.date >= dt_from) & (dt_series.dt.date <= dt_to)
    dfv = df[mask].copy().sort_values(["발주번호", "품목코드"])

    orders = dfv.groupby("발주번호").agg(
        건수=("품목코드", "count"),
        총수량=("수량", lambda x: pd.to_numeric(x, errors="coerce").fillna(0).sum()),
        총금액=("금액", lambda x: pd.to_numeric(x, errors="coerce").fillna(0).sum()),
        상태=("상태", lambda s: "출고완료" if (s == "출고완료").all() else "접수")
    ).reset_index()

    tbl, pick = st.columns([3, 1])
    with tbl:
        st.dataframe(
            orders.rename(columns={"총금액": "총 금액", "총수량": "총 수량"}),
            use_container_width=True, height=220,
            column_config={
                "총 금액": st.column_config.NumberColumn(format="%,d"),
                "총 수량": st.column_config.NumberColumn(format="%,d")
            }
        )
    with pick:
        target_order = st.radio("발주번호", options=orders["발주번호"].tolist(), key="store_edit_pick")

    if not target_order:
        return

    target_df = dfv[dfv["발주번호"] == target_order].copy()
    is_ship_done = (target_df["상태"] == "출고완료").all()

    st.caption(f"선택 발주 품목수: {len(target_df)}  |  상태: {'출고완료' if is_ship_done else '접수'}")
    show_cols = ["품목코드", "품목명", "단위", "수량", "단가", "비고"]

    if is_ship_done:
        st.info("출고완료 건은 수정/삭제할 수 없습니다.")
        st.dataframe(
            target_df[show_cols], use_container_width=True, height=360,
            column_config={
                "단가": st.column_config.NumberColumn(label="단가(원)", format="%,d"),
                "수량": st.column_config.NumberColumn(format="%,d")
            }
        )
        return

    target_df["삭제"] = False
    edited = st.data_editor(
        target_df[["발주번호"] + show_cols + ["삭제"]],
        disabled=["발주번호"],
        column_config={
            "수량": st.column_config.NumberColumn(min_value=0, step=1, format="%,d"),
            "단가": st.column_config.NumberColumn(label="단가(원)", format="%,d", step=1),
            "삭제": st.column_config.CheckboxColumn()
        },
        use_container_width=True, num_rows="dynamic", hide_index=True, key="store_edit_orders_editor"
    )

    col_a, col_b = st.columns([1, 1])
    with col_a:
        if st.button("선택 행 삭제", key="store_del_rows"):
            base = df.copy()
            to_del = edited[edited["삭제"] == True][["발주번호", "품목코드"]]  # noqa: E712
            if not to_del.empty:
                drop_idx = pd.MultiIndex.from_frame(to_del)
                base_idx = pd.MultiIndex.from_frame(base[["발주번호", "품목코드"]])
                keep_mask = ~base_idx.isin(drop_idx)
                ok = write_orders_df(base.loc[keep_mask].copy())
                st.success("선택 행을 삭제했습니다.") if ok else st.error("삭제 실패")
            else:
                st.info("삭제할 행이 선택되지 않았습니다.")

    with col_b:
        if st.button("변경 내용 저장", type="primary", key="store_edit_save"):
            base = df.copy()
            key_cols = ["발주번호", "품목코드"]
            merged = base.merge(
                edited[key_cols + ["수량", "단가", "비고", "삭제"]],
                on=key_cols, how="left", suffixes=("", "_new")
            )
            base["수량"] = merged["수량_new"].combine_first(base["수량"])
            base["단가"] = merged["단가_new"].combine_first(base["단가"])
            base["비고"] = merged["비고_new"].combine_first(base["비고"])
            del_mask = (merged["삭제"] == True) | (
                pd.to_numeric(base["수량"], errors="coerce").fillna(0).astype(int) == 0
            )
            base = base[~(del_mask.fillna(False))].copy()
            base["수량"] = pd.to_numeric(base["수량"], errors="coerce").fillna(0).astype(int)
            base["단가"] = pd.to_numeric(base["단가"], errors="coerce").fillna(0).astype(int)
            base["금액"] = (base["수량"] * base["단가"]).astype(int)
            ok = write_orders_df(base)
            st.success("변경사항을 저장했습니다.") if ok else st.error("저장 실패")


# =============================================================================
# 9) 발주서 조회·다운로드 — 정리본
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

    st.dataframe(
        dfv, use_container_width=True, height=420,
        column_config={
            "단가": st.column_config.NumberColumn(label="단가(원)", format="%,d"),
            "금액": st.column_config.NumberColumn(label="총금액(원)", format="%,d"),
            "수량": st.column_config.NumberColumn(format="%,d"),
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
# 10) 발주 품목 가격 조회 — 정리본
# =============================================================================
def page_store_master_view(master_df: pd.DataFrame):
    st.subheader("🏷️ 발주 품목 가격 조회")
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

    cols = [c for c in ["품목코드", "품목명", "분류", "단위", "단가"] if c in master_df.columns]
    view = master_df[cols].copy()
    view["단가"] = pd.to_numeric(view.get("단가", 0), errors="coerce").fillna(0).astype(int)

    st.dataframe(
        view, use_container_width=True, height=480,
        column_config={"단가": st.column_config.NumberColumn(label="단가(원)", format="%,d")}
    )


# =============================================================================
# 8) 관리자 화면
# =============================================================================
def page_admin_orders_manage(master_df: pd.DataFrame):
    st.subheader("🗂️ 주문 관리 · 출고확인")
    df = load_orders_df().copy()
    if df.empty:
        st.info("발주 데이터가 없습니다."); return

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

    st.caption(f"조회 건수: {len(dfv):,}건")
    st.dataframe(dfv, use_container_width=True, height=420)
    st.download_button("CSV 다운로드",
                       data=dfv.to_csv(index=False).encode("utf-8-sig"),
                       file_name="orders_admin.csv",
                       mime="text/csv",
                       key="admin_mng_csv")

    st.markdown("---")
    st.markdown("**출고 처리 (이미 출고완료된 발주번호는 목록 제외)**")
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

    c1, c2 = st.columns(2)
    with c1: dt_from = st.date_input("시작일", value=date.today()-timedelta(days=7), key="admin_ship_from")
    with c2: dt_to   = st.date_input("종료일", value=date.today(), key="admin_ship_to")

    dt_series = pd.to_datetime(df["주문일시"], errors="coerce")
    mask = dt_series.notna() & (dt_series.dt.date >= dt_from) & (dt_series.dt.date <= dt_to)
    dfv = df[mask].copy().sort_values(["발주번호","품목코드"])

    st.caption(f"조회 건수: {len(dfv):,}건")
    orders = dfv.groupby("발주번호").agg(건수=("품목코드","count"),
                                      상태=("상태", lambda s: "출고완료" if (s=="출고완료").all() else "접수")).reset_index()
    st.dataframe(orders, use_container_width=True, height=220)

    st.markdown("---")
    st.markdown("**출고 상태 일괄 변경 (발주번호 단위)**")
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
    edited = st.data_editor(
        view, use_container_width=True, hide_index=True, num_rows="dynamic",
        column_config={
            "단가": st.column_config.NumberColumn(label="단위당 단가", min_value=0, step=1, format="%,d"),
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
    # 로그인 전에는 상단 제목을 표시하지 않음 (중복 표기 방지)
    if not require_login():
        st.stop()

    # 로그인 후에만 제목 표시 (버전 문구 제거)
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
        t1, t2, t3, t4 = st.tabs(["발주 등록·확인", "발주 조회·변경", "발주서 다운로드", "발주 품목 가격 조회"])
        with t1: page_store_register_confirm(master)
        with t2: page_store_orders_change()
        with t3: page_store_order_form_download(master)
        with t4: page_store_master_view(master)
