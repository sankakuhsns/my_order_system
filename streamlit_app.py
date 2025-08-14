# -*- coding: utf-8 -*-
# =============================================================================
# 📦 Streamlit 식자재 발주 시스템 (v4.1 - 최종 안정화판)
# - 주요 개선사항:
#   - NameError 해결: 누락되었던 유틸리티 함수(make_order_sheet_excel) 복원
#   - 전체 기능 및 로직 최종 검토 및 안정화
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

THEME = { "BORDER": "#e8e8ee", "PRIMARY": "#1C6758", "BG": "#f7f8fa", "CARD_BG": "#ffffff", "TEXT": "#222", "MUTED": "#777" }
CARD_STYLE = f"background-color:{THEME['CARD_BG']}; border:1px solid {THEME['BORDER']}; border-radius:12px; padding:16px;"

st.markdown(f"""
<style>
html, body, [data-testid="stAppViewContainer"] {{ background: {THEME['BG']}; color: {THEME['TEXT']}; }}
.small {{ font-size:12px; color:{THEME['MUTED']}; }}
.block-container {{ padding-top: 2.4rem; padding-bottom: 1.6rem; }}
.card {{ {CARD_STYLE} box-shadow: 0 2px 8px rgba(0,0,0,0.03); }}
[data-testid="stAppViewContainer"] .main .block-container {{ max-width: 1050px; margin: 0 auto; padding: 0 12px; }}
.stTextInput>div>div>input, .stNumberInput input, .stDateInput input {{ border:1px solid {THEME['BORDER']} !important; border-radius:10px !important; height:34px; }}
.stTabs [role="tablist"] {{ display:flex !important; gap:12px !important; flex-wrap:wrap !important; margin:8px 0 24px !important; border-bottom:none !important; }}
.stTabs button[role="tab"] {{ border:1px solid {THEME['BORDER']} !important; border-radius:12px !important; background:#fff !important; padding:10px 14px !important; box-shadow:0 1px 6px rgba(0,0,0,0.04) !important; cursor:pointer !important; transition: transform .08s ease, box-shadow .12s ease; }}
.stTabs button[role="tab"]:hover {{ transform: translateY(-1px); box-shadow:0 4px 12px rgba(0,0,0,0.08); }}
.stTabs button[role="tab"][aria-selected="true"] {{ border-color:{THEME['PRIMARY']} !important; color:{THEME['PRIMARY']} !important; box-shadow:0 6px 16px rgba(28,103,88,0.18) !important; font-weight:700; }}
.stTabs [data-baseweb="tab-highlight"] {{ display:none !important; }}
.login-title {{ text-align:center; font-size:42px; font-weight:800; margin:16px 0 12px; }}
.stButton > button[data-testid="baseButton-primary"] {{ background: #1C6758 !important; color: #fff !important; border: 1px solid #1C6758 !important; border-radius: 10px !important; height: 34px !important; }}
.flat-container [data-testid="stDataFrame"] {{ border: none; box-shadow: none; }}
</style>
""", unsafe_allow_html=True)

# --- 공용 작은 UI 유틸 ---
def v_spacer(height: int):
    st.markdown(f"<div style='height:{height}px'></div>", unsafe_allow_html=True)

KST = ZoneInfo("Asia/Seoul")
def now_kst_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str: return datetime.now(KST).strftime(fmt)

# =============================================================================
# 1) Users 로더
# =============================================================================
def _normalize_account(uid: str, payload: Mapping) -> dict:
    pwd_plain, pwd_hash = payload.get("password"), payload.get("password_hash")
    name = str(payload.get("name", uid)).strip()
    role = str(payload.get("role", "store")).strip().lower()
    if not (pwd_plain or pwd_hash): st.error(f"[users.{uid}]에 password 또는 password_hash가 필요합니다."); st.stop()
    if role not in {"store", "admin"}: st.error(f"[users.{uid}].role 은 'store' 또는 'admin' 이어야 합니다. (현재: {role})"); st.stop()
    return {"password": str(pwd_plain) if pwd_plain else None, "password_hash": str(pwd_hash).lower() if pwd_hash else None, "name": name, "role": role}

@st.cache_data
def load_users_from_secrets() -> Dict[str, Dict[str, str]]:
    cleaned: Dict[str, Dict[str, str]] = {}
    users_root = st.secrets.get("users", None)
    if isinstance(users_root, Mapping) and len(users_root) > 0:
        for uid, payload in users_root.items():
            if isinstance(payload, Mapping): cleaned[str(uid)] = _normalize_account(str(uid), payload)
    elif isinstance(users_root, list) and users_root:
        for row in users_root:
            if not isinstance(row, Mapping): continue
            uid = row.get("user_id") or row.get("uid") or row.get("id")
            if uid: cleaned[str(uid)] = _normalize_account(str(uid), row)
    if not cleaned: st.error("로그인 계정을 찾을 수 없습니다. Secrets 의 [users] 구조를 확인하세요."); st.stop()
    return cleaned

USERS = load_users_from_secrets()

# =============================================================================
# 2) 시트/스키마 정의
# =============================================================================
SHEET_NAME_MASTER = "상품마스터"
SHEET_NAME_ORDERS = "발주"
CART_COLUMNS = ["품목코드","품목명","단위","단가","수량","금액"]
ORDERS_COLUMNS = ["주문일시","발주번호","지점ID","지점명","납품요청일","품목코드","품목명","단위","수량","단가","금액","비고","상태","처리일시","처리자"]

# =============================================================================
# 3) Google Sheets 연결
# =============================================================================
@st.cache_resource(show_spinner=False)
def get_gs_client():
    google = st.secrets.get("google", {})
    required = ["type","project_id","private_key_id","private_key","client_email","client_id"]
    if missing := [k for k in required if not str(google.get(k, "")).strip()]: st.error(f"Google 연동 설정 부족: {', '.join(missing)}"); st.stop()
    creds_info = dict(google)
    if "\\n" in str(creds_info.get("private_key", "")): creds_info["private_key"] = str(creds_info["private_key"]).replace("\\n", "\n")
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def open_spreadsheet():
    key = str(st.secrets.get("google", {}).get("SPREADSHEET_KEY") or st.secrets.get("SPREADSHEET_KEY", "")).strip()
    if not key: st.error("Secrets 에 SPREADSHEET_KEY가 없습니다."); st.stop()
    try: return get_gs_client().open_by_key(key)
    except Exception as e: st.error(f"스프레드시트 열기 실패: {e}"); st.stop()

# =============================================================================
# 4) 데이터 I/O
# =============================================================================
@st.cache_data(ttl=180)
def load_master_df() -> pd.DataFrame:
    try: ws = open_spreadsheet().worksheet(SHEET_NAME_MASTER)
    except gspread.WorksheetNotFound: return pd.DataFrame()
    df = pd.DataFrame(ws.get_all_records())
    for c in ["품목코드","품목명","단위","분류","단가","활성"]:
        if c not in df.columns: df[c] = (0 if c=="단가" else (True if c=="활성" else ""))
    if "활성" in df.columns:
        mask = df["활성"].astype(str).str.lower().isin(["1","true","y","yes"])
        df = df[mask | df["활성"].isna()]
    df["단가"] = pd.to_numeric(df["단가"], errors="coerce").fillna(0).astype(int)
    return df

def write_master_df(df: pd.DataFrame) -> bool:
    cols = [c for c in ["품목코드","품목명","분류","단위","단가","활성"] if c in df.columns]
    try:
        sh = open_spreadsheet(); ws = sh.worksheet(SHEET_NAME_MASTER)
        ws.clear(); ws.update("A1", [cols] + df[cols].fillna("").values.tolist(), value_input_option='USER_ENTERED')
        load_master_df.clear(); return True
    except Exception as e: st.error(f"상품마스터 저장 실패: {e}"); return False

@st.cache_data(ttl=60)
def load_orders_df() -> pd.DataFrame:
    try: ws = open_spreadsheet().worksheet(SHEET_NAME_ORDERS)
    except gspread.WorksheetNotFound: return pd.DataFrame(columns=ORDERS_COLUMNS)
    df = pd.DataFrame(ws.get_all_records())
    for c in ORDERS_COLUMNS:
        if c not in df.columns: df[c] = ""
    for c in ["수량","단가","금액"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df[ORDERS_COLUMNS].copy()

def write_orders_df(df: pd.DataFrame) -> bool:
    try:
        sh = open_spreadsheet(); ws = sh.worksheet(SHEET_NAME_ORDERS)
        ws.clear(); ws.update("A1", [ORDERS_COLUMNS] + df[ORDERS_COLUMNS].fillna("").values.tolist(), value_input_option='USER_ENTERED')
        load_orders_df.clear(); return True
    except Exception as e: st.error(f"발주 저장 실패: {e}"); return False

def append_orders(rows: List[Dict[str, Any]]) -> bool:
    if not rows: return True
    try:
        sh = open_spreadsheet(); ws = sh.worksheet(SHEET_NAME_ORDERS)
        values_to_add = [[r.get(col, "") for col in ORDERS_COLUMNS] for r in rows]
        ws.append_rows(values_to_add, value_input_option='USER_ENTERED')
        load_orders_df.clear(); return True
    except Exception as e: st.error(f"발주 추가 실패: {e}"); return False

def update_order_status(selected_ids: List[str], new_status: str, handler: str) -> bool:
    df = load_orders_df(); now = now_kst_str()
    mask = df["발주번호"].astype(str).isin([str(x) for x in selected_ids])
    df.loc[mask, "상태"] = new_status
    df.loc[mask, "처리일시"] = now
    df.loc[mask, "처리자"] = handler
    return write_orders_df(df)

# =============================================================================
# 5) 로그인
# =============================================================================
def require_login():
    if st.session_state.get("auth", {}).get("login"): return True
    st.markdown('<div class="login-title">식자재 발주 시스템</div>', unsafe_allow_html=True)
    _, mid, _ = st.columns([3, 2, 3])
    with mid.form("login_form"):
        uid = st.text_input("아이디 또는 지점명", key="login_uid", placeholder="예: jeondae / 전대점")
        pwd = st.text_input("비밀번호", type="password", key="login_pw")
        if st.form_submit_button("로그인", use_container_width=True):
            real_uid, acct = _find_account(uid)
            if not (real_uid and acct and verify_password(pwd, acct.get("password_hash"), acct.get("password"))):
                st.error("아이디(또는 지점명) 또는 비밀번호가 올바르지 않습니다.")
            else:
                st.session_state["auth"] = {"login": True, "user_id": real_uid, "name": acct["name"], "role": acct["role"]}
                st.rerun()
    return False

def verify_password(input_pw: str, stored_hash: Optional[str], fallback_plain: Optional[str]) -> bool:
    if stored_hash: return hashlib.sha256(input_pw.encode()).hexdigest() == stored_hash.strip().lower().split("$", 1)[-1]
    return str(input_pw) == str(fallback_plain) if fallback_plain is not None else False

def _find_account(uid_or_name: str):
    s_lower = str(uid_or_name or "").strip().lower()
    if not s_lower: return None, None
    for uid, acct in USERS.items():
        if uid.lower() == s_lower or acct.get("name", "").lower() == s_lower: return uid, acct
    return None, None

# =============================================================================
# 6) 유틸 - [누락된 함수 복원]
# =============================================================================
def make_order_id(store_id: str) -> str: return f"{datetime.now(KST):%Y%m%d%H%M%S}{store_id}"

def make_order_sheet_excel(df_note: pd.DataFrame, include_price: bool, *, title: str = "산카쿠 납품내역서") -> BytesIO:
    buf = BytesIO()
    cols = ["발주번호","주문일시","납품요청일","지점명","품목코드","품목명","단위","수량","비고","상태"]
    if include_price: cols += ["단가","금액"]
    
    export = df_note[cols].copy().sort_values(["발주번호","품목코드"])
    for col in ["수량", "단가", "금액"]:
        if col in export.columns:
            export[col] = pd.to_numeric(export[col], errors="coerce").fillna(0)

    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        export.to_excel(w, index=False, sheet_name="내역", startrow=2)
        wb, ws = w.book, w.sheets["내역"]
        fmt = { "title": wb.add_format({"bold": True, "font_size": 16, "align":"center"}), "th": wb.add_format({"bold": True, "bg_color":"#F2F2F2", "border":1}) }
        ws.merge_range(0, 0, 0, len(export.columns)-1, title, fmt["title"])
    buf.seek(0)
    return buf

# =============================================================================
# 🛒 장바구니 유틸(전역)
# =============================================================================
def init_session_state():
    defaults = { "cart": pd.DataFrame(columns=CART_COLUMNS), "store_editor_ver": 0, "cart_selected_codes": [], "store_selected_orders": [], "admin_pending_selection": [], "admin_shipped_selection": [] }
    for key, value in defaults.items():
        if key not in st.session_state: st.session_state[key] = value

def coerce_cart_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in CART_COLUMNS:
        if col not in out.columns: out[col] = 0 if col in ["단가","수량","금액"] else ""
    for c in ["단가","수량"]:
        out[c] = pd.to_numeric(out[c].astype(str).str.replace(",", "").str.strip(), errors="coerce").fillna(0).astype(int)
    out["금액"] = out["단가"] * out["수량"]
    return out[CART_COLUMNS]

def add_to_cart(rows_df: pd.DataFrame):
    add = rows_df[rows_df["수량"] > 0]
    if add.empty: return
    cart = st.session_state.cart.copy()
    merged = pd.concat([cart, add]).groupby("품목코드", as_index=False).agg({"품목명": "last", "단위": "last", "단가": "last", "수량": "sum"})
    merged["금액"] = merged["단가"] * merged["수량"]
    st.session_state.cart = merged[CART_COLUMNS]

def remove_from_cart(codes: list[str]):
    if codes: st.session_state.cart = st.session_state.cart[~st.session_state.cart["품목코드"].isin(codes)]

# =============================================================================
# 🛒 발주(지점) 화면
# =============================================================================
def page_store_register_confirm(master_df: pd.DataFrame):
    st.subheader("🛒 발주 요청")
    v_spacer(10)
    with st.container(border=True):
        st.markdown("##### 🗓️ 납품 요청 정보")
        today, max_day = date.today(), date.today() + timedelta(days=7)
        c1, c2 = st.columns([1, 1.2])
        quick = c1.radio("납품 선택", ["오늘", "내일", "직접선택"], horizontal=True, key="store_quick_radio", label_visibility="collapsed")
        if quick == "오늘": 납품요청일 = today
        elif quick == "내일": 납품요청일 = today + timedelta(days=1)
        else: 납품요청일 = c2.date_input("납품 요청일", value=today, min_value=today, max_value=max_day, key="store_req_date", label_visibility="collapsed")
        memo = st.text_area("요청 사항(선택)", key="store_req_memo", height=80, placeholder="예) 입고 시 얼음팩 추가 부탁드립니다.")
    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 🧾 발주 수량 입력")
        l, r = st.columns([2, 1])
        keyword = l.text_input("품목 검색(이름/코드)", key="store_kw", placeholder="오이, P001 등")
        cat_opt = ["(전체)"] + sorted(master_df["분류"].dropna().unique().tolist())
        cat_sel = r.selectbox("분류(선택)", cat_opt, key="store_cat_sel")
        df_view = master_df.copy()
        if keyword: df_view = df_view[df_view.apply(lambda row: keyword.strip().lower() in str(row["품목명"]).lower() or keyword.strip().lower() in str(row["품목코드"]).lower(), axis=1)]
        if cat_sel != "(전체)": df_view = df_view[df_view["분류"] == cat_sel]
        st.markdown("<div class='flat-container'>", unsafe_allow_html=True)
        with st.form(key="add_to_cart_form"):
            df_edit = df_view[["품목코드","품목명","단위","단가"]].copy(); df_edit["수량"] = ""
            edited_disp = st.data_editor(df_edit, key=f"editor_v{st.session_state.store_editor_ver}", hide_index=True, disabled=["품목코드","품목명","단위","단가"], use_container_width=True,
                column_config={"단가": st.column_config.NumberColumn("단가", format="%d"), "수량": st.column_config.TextColumn("수량")})
            if st.form_submit_button("장바구니 추가", use_container_width=True, type="primary"):
                items_to_add = coerce_cart_df(edited_disp)
                if not items_to_add[items_to_add["수량"] > 0].empty:
                    add_to_cart(items_to_add)
                    st.session_state.store_editor_ver += 1
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 🧺 장바구니")
        cart = st.session_state.cart
        if not cart.empty:
            cart_disp = cart.copy(); cart_disp.insert(0, "선택", cart_disp["품목코드"].isin(st.session_state.cart_selected_codes))
            edited_cart = st.data_editor(cart_disp, key="cart_editor", hide_index=True, disabled=["품목코드","품목명","단위","금액"],
                column_config={"단가": st.column_config.NumberColumn("단가", format="%d"), "금액": st.column_config.NumberColumn("금액", format="%d")})
            st.session_state.cart_selected_codes = edited_cart[edited_cart["선택"]]["품목코드"].tolist()
            st.session_state.cart = coerce_cart_df(edited_cart.drop(columns=["선택"]))
            c1, c2, c3, _ = st.columns([1.2,1,1,4])
            is_all_selected = set(st.session_state.cart_selected_codes) == set(cart["품목코드"].tolist()) and not cart.empty
            if c1.button("전체 해제" if is_all_selected else "전체 선택", use_container_width=True):
                st.session_state.cart_selected_codes = [] if is_all_selected else cart["품목코드"].tolist(); st.rerun()
            if c2.button("선택 삭제", use_container_width=True, disabled=not st.session_state.cart_selected_codes):
                remove_from_cart(st.session_state.cart_selected_codes); st.session_state.cart_selected_codes = []; st.rerun()
            if c3.button("비우기", use_container_width=True):
                st.session_state.cart = pd.DataFrame(columns=CART_COLUMNS); st.session_state.cart_selected_codes = []; st.rerun()
        else: st.info("장바구니가 비어 있습니다.")
    v_spacer(16)
    with st.form("submit_form"):
        cart_now = st.session_state.cart
        st.markdown(f"**최종 확인:** 총 {len(cart_now)}개 품목, 합계 {cart_now['금액'].sum():,}원")
        confirm = st.checkbox("위 내용으로 발주를 제출합니다.")
        if st.form_submit_button("📦 발주 제출", type="primary", use_container_width=True, disabled=cart_now.empty):
            if not confirm: st.warning("제출 확인 체크박스를 선택해주세요."); st.stop()
            user, order_id = st.session_state.auth, make_order_id(st.session_state.auth["user_id"])
            rows = [{"주문일시": now_kst_str(), "발주번호": order_id, "지점ID": user["user_id"], "지점명": user["name"], "납품요청일": f"{납품요청일:%Y-%m-%d}", "비고": memo, "상태": "접수", **r.to_dict()} for _, r in cart_now.iterrows()]
            if append_orders(rows):
                st.success("발주가 성공적으로 제출되었습니다."); st.session_state.cart = pd.DataFrame(columns=CART_COLUMNS); st.rerun()
            else: st.error("발주 제출 중 오류가 발생했습니다.")

# ──────────────────────────────────────────────
# 🧾 발주 조회/수정 (지점)
# ──────────────────────────────────────────────
def page_store_orders_change():
    st.subheader("🧾 발주 조회 · 수정")
    df_all, user = load_orders_df(), st.session_state.auth
    df_user = df_all[df_all["지점ID"] == user["user_id"]]
    if df_user.empty: st.info("발주 데이터가 없습니다."); return
    orders = df_user.groupby("발주번호").agg(주문일시=("주문일시", "first"), 건수=("품목코드", "count"), 금액=("금액", "sum"), 상태=("상태", "first")).reset_index().sort_values("주문일시", ascending=False)
    pending = orders[orders["상태"] == "접수"]; done = orders[orders["상태"] == "출고완료"]
    with st.container(border=True):
        st.markdown("##### 📦 발주 리스트")
        disp_df = pd.concat([pending, done]).copy(); disp_df.insert(0, "선택", disp_df["발주번호"].isin(st.session_state.store_selected_orders))
        edited_df = st.data_editor(disp_df, key="store_orders_editor", hide_index=True, disabled=orders.columns, column_config={"금액": st.column_config.NumberColumn("금액", format="%d")})
        st.session_state.store_selected_orders = edited_df[edited_df["선택"]]["발주번호"].tolist()
        if st.button("선택 발주 삭제", disabled=not any(pid in pending["발주번호"].tolist() for pid in st.session_state.store_selected_orders)):
            if write_orders_df(df_all[~df_all["발주번호"].isin(st.session_state.store_selected_orders)]):
                st.success("선택한 발주가 삭제되었습니다."); st.session_state.store_selected_orders = []; st.rerun()
    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 📄 발주품목조회")
        if len(st.session_state.store_selected_orders) == 1:
            target_df = df_user[df_user["발주번호"] == st.session_state.store_selected_orders[0]]
            st.dataframe(target_df[ORDERS_COLUMNS[5:12]], hide_index=True, use_container_width=True, column_config={"단가": st.column_config.NumberColumn("단가", format="%d"),"금액": st.column_config.NumberColumn("금액", format="%d")})
            buf = make_order_sheet_excel(target_df, include_price=True, title=f"발주서 ({st.session_state.store_selected_orders[0]})")
            st.download_button("이 발주서 다운로드", data=buf, file_name=f"발주서_{st.session_state.store_selected_orders[0]}.xlsx", mime="application/vnd.ms-excel", use_container_width=True)
        else: st.info("상세 내용을 보려면 위 목록에서 발주를 하나만 선택하세요.")

# ──────────────────────────────────────────────
# 📑 발주서 다운로드 (지점)
# ──────────────────────────────────────────────
def page_store_order_form_download():
    st.subheader("📑 발주서 다운로드")
    df = load_orders_df()[load_orders_df()["지점ID"] == st.session_state.auth["user_id"]]
    if df.empty: st.info("발주 데이터가 없습니다."); return
    with st.container(border=True):
        st.markdown("##### 🔎 조회 조건")
        c1, c2 = st.columns(2)
        dt_from = c1.date_input("시작일", date.today() - timedelta(days=7), key="store_dl_from")
        dt_to = c2.date_input("종료일", date.today(), key="store_dl_to")
    dfv = df[(pd.to_datetime(df["주문일시"]).dt.date >= dt_from) & (pd.to_datetime(df["주문일시"]).dt.date <= dt_to)].copy().sort_values(["발주번호", "품목코드"])
    with st.container(border=True):
        st.markdown("##### 📄 미리보기 및 다운로드")
        st.dataframe(dfv, use_container_width=True, height=420, column_config={"단가": st.column_config.NumberColumn("단가", format="%d"),"금액": st.column_config.NumberColumn("금액", format="%d")})
        if not dfv.empty:
            buf = make_order_sheet_excel(dfv, include_price=True, title="산카쿠 발주서")
            st.download_button("엑셀 다운로드", data=buf, file_name=f"발주서_{dt_from}~{dt_to}.xlsx", mime="application/vnd.ms-excel", use_container_width=True)

# ──────────────────────────────────────────────
# 🏷️ 품목 가격 조회 (지점)
# ──────────────────────────────────────────────
def page_store_master_view(master_df: pd.DataFrame):
    st.subheader("🏷️ 발주 품목 가격 조회")
    st.dataframe(master_df[["품목코드", "품목명", "분류", "단위", "단가"]], use_container_width=True, hide_index=True, column_config={"단가": st.column_config.NumberColumn("단가", format="%d")})

# ──────────────────────────────────────────────
# 🗂️ 발주요청조회 · 수정 (관리자)
# ──────────────────────────────────────────────
def page_admin_unified_management():
    st.subheader("🗂️ 발주요청조회 · 수정")
    df_all = load_orders_df()
    if df_all.empty: st.info("발주 데이터가 없습니다."); return
    with st.container(border=True):
        st.markdown("##### 🔎 조회 조건")
        c1, c2, c3 = st.columns([1,1,2])
        dt_from = c1.date_input("시작일", date.today()-timedelta(days=7), key="admin_mng_from")
        dt_to = c2.date_input("종료일", date.today(), key="admin_mng_to")
        stores = ["(전체)"] + sorted(df_all["지점명"].dropna().unique().tolist())
        store = c3.selectbox("지점", stores, key="admin_mng_store")
    df = df_all[(pd.to_datetime(df_all["주문일시"]).dt.date >= dt_from) & (pd.to_datetime(df_all["주문일시"]).dt.date <= dt_to)]
    if store != "(전체)": df = df[df["지점명"] == store]
    orders = df.groupby("발주번호").agg(주문일시=("주문일시", "first"), 지점명=("지점명", "first"), 건수=("품목코드", "count"), 금액=("금액", "sum"), 상태=("상태", "first")).reset_index().sort_values("주문일시", ascending=False)
    pending = orders[orders["상태"] == "접수"]; shipped = orders[orders["상태"] == "출고완료"]
    tab1, tab2 = st.tabs([f"📦 발주 요청 접수 ({len(pending)}건)", f"✅ 출고 완료 ({len(shipped)}건)"])
    with tab1:
        disp_df = pending.copy(); disp_df.insert(0, "선택", disp_df["발주번호"].isin(st.session_state.admin_pending_selection))
        edited_df = st.data_editor(disp_df, key="admin_pending_editor", hide_index=True, disabled=orders.columns, column_config={"금액": st.column_config.NumberColumn("금액", format="%d")})
        st.session_state.admin_pending_selection = edited_df[edited_df["선택"]]["발주번호"].tolist()
        if st.button("✅ 선택 발주 출고", type="primary", disabled=not st.session_state.admin_pending_selection):
            if update_order_status(st.session_state.admin_pending_selection, "출고완료", st.session_state.auth["name"]):
                st.success(f"{len(st.session_state.admin_pending_selection)}건이 출고 처리되었습니다."); st.session_state.admin_pending_selection = []; st.rerun()
    with tab2:
        disp_df = shipped.copy(); disp_df.insert(0, "선택", disp_df["발주번호"].isin(st.session_state.admin_shipped_selection))
        edited_df = st.data_editor(disp_df, key="admin_shipped_editor", hide_index=True, disabled=orders.columns, column_config={"금액": st.column_config.NumberColumn("금액", format="%d")})
        st.session_state.admin_shipped_selection = edited_df[edited_df["선택"]]["발주번호"].tolist()
        if st.button("↩️ 접수 상태로 변경", disabled=not st.session_state.admin_shipped_selection):
            if update_order_status(st.session_state.admin_shipped_selection, "접수", st.session_state.auth["name"]):
                st.success(f"{len(st.session_state.admin_shipped_selection)}건이 접수 상태로 변경되었습니다."); st.session_state.admin_shipped_selection = []; st.rerun()
    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 📄 발주품목확인")
        total_selection = st.session_state.admin_pending_selection + st.session_state.admin_shipped_selection
        if len(total_selection) == 1:
            target_df = df_all[df_all["발주번호"] == total_selection[0]]
            st.dataframe(target_df[ORDERS_COLUMNS[5:12]], hide_index=True, use_container_width=True, column_config={"단가": st.column_config.NumberColumn("단가", format="%d"),"금액": st.column_config.NumberColumn("금액", format="%d")})
            buf = make_order_sheet_excel(target_df, include_price=True, title=f"출고내역서 ({total_selection[0]})")
            st.download_button("이 내역서 다운로드", data=buf, file_name=f"출고내역서_{total_selection[0]}.xlsx", mime="application/vnd.ms-excel", use_container_width=True)
        else: st.info("상세 내용을 보려면 위 목록에서 발주를 하나만 선택하세요.")

# ──────────────────────────────────────────────
# 📑 출고 내역서 다운로드 (관리자)
# ──────────────────────────────────────────────
def page_admin_delivery_note():
    st.subheader("📑 출고 내역서 다운로드")
    df = load_orders_df()
    if df.empty: st.info("발주 데이터가 없습니다."); return
    with st.container(border=True):
        st.markdown("##### 🔎 조회 조건")
        c1, c2, c3, c4 = st.columns(4)
        dt_from = c1.date_input("시작일", date.today()-timedelta(days=7), key="admin_dl_from")
        dt_to = c2.date_input("종료일", date.today(), key="admin_dl_to")
        stores = ["(전체)"] + sorted(df["지점명"].dropna().unique().tolist())
        store = c3.selectbox("지점", stores, key="admin_dl_store")
        order_ids = ["(전체)"] + sorted(df["발주번호"].dropna().unique().tolist(), reverse=True)
        target_order = c4.selectbox("발주번호", order_ids, key="admin_dl_orderid")
    mask = (pd.to_datetime(df["주문일시"]).dt.date >= dt_from) & (pd.to_datetime(df["주문일시"]).dt.date <= dt_to)
    if store != "(전체)": mask &= (df["지점명"]==store)
    if target_order != "(전체)": mask &= (df["발주번호"] == target_order)
    dfv = df[mask].copy().sort_values(["지점명", "발주번호", "품목코드"])
    st.dataframe(dfv, hide_index=True)
    if not dfv.empty:
        buf = make_order_sheet_excel(dfv, include_price=True, title="산카쿠 출고내역서")
        st.download_button("엑셀 다운로드", data=buf, file_name=f"출고내역서_{dt_from}~{dt_to}.xlsx", mime="application/vnd.ms-excel", use_container_width=True)

# ──────────────────────────────────────────────
# 🏷️ 납품 품목 가격 설정 (관리자)
# ──────────────────────────────────────────────
def page_admin_items_price(master_df: pd.DataFrame):
    st.subheader("🏷️ 납품 품목 가격 설정")
    st.caption("가격을 수정하거나 품목을 추가/삭제한 후 저장 버튼을 누르세요.")
    with st.form("master_edit_form"):
        edited = st.data_editor(master_df.assign(삭제=False), hide_index=True, num_rows="dynamic", use_container_width=True,
            column_config={"단가": st.column_config.NumberColumn("단가", format="%d")})
        if st.form_submit_button("변경사항 저장", type="primary", use_container_width=True):
            final_df = edited[~edited["삭제"]].drop(columns=["삭제"])
            if write_master_df(final_df):
                st.success("상품마스터가 저장되었습니다."); st.rerun()

# =============================================================================
# 라우팅
# =============================================================================
if __name__ == "__main__":
    if not require_login(): st.stop()
    init_session_state()
    st.title("📦 식자재 발주 시스템")
    user, master = st.session_state.auth, load_master_df()
    if user["role"] == "admin":
        tabs = st.tabs(["🗂️ 발주요청조회·수정", "📑 출고 내역서 다운로드", "🏷️ 납품 품목 가격 설정"])
        with tabs[0]: page_admin_unified_management()
        with tabs[1]: page_admin_delivery_note()
        with tabs[2]: page_admin_items_price(master)
    else:
        tabs = st.tabs(["🛒 발주 요청", "🧾 발주 조회·수정", "📑 발주서 다운로드", "🏷️ 발주 품목 가격 조회"])
        with tabs[0]: page_store_register_confirm(master)
        with tabs[1]: page_store_orders_change()
        with tabs[2]: page_store_order_form_download()
        with tabs[3]: page_store_master_view(master)
