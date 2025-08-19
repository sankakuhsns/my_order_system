# -*- coding: utf-8 -*-
# =============================================================================
# 📦 Streamlit 식자재 발주 시스템 (v6.0 - 전체 기능 개편)
#
# - 주요 개선사항:
#   - 4개 시트(지점마스터, 상품마스터, 발주, 변경로그) 기반 데이터 구조 전면 개편
#   - 관리자용 '매출 조회' 탭 신설 (기간별/지점별 조회 및 시각화)
#   - '증빙서류 다운로드' 기능 구체화 (거래명세서, 세금계산서 등)
#   - 상품마스터 변경 이력 추적을 위한 '변경로그' 기능 추가
#   - 발주 데이터의 금액을 '공급가액', '세액', '합계금액'으로 세분화하여 관리
# =============================================================================

from io import BytesIO
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional
from collections.abc import Mapping
from zoneinfo import ZoneInfo
import math

import hashlib
import pandas as pd
import streamlit asst

# Google Sheets
import gspread
from google.oauth2 import service_account

# Excel export
import xlsxwriter

# -----------------------------------------------------------------------------
# 페이지/테마/스타일 (기존과 동일)
# -----------------------------------------------------------------------------
st.set_page_config(page_title="산카쿠 식자재 발주 시스템", page_icon="📦", layout="wide")

THEME = { "BORDER": "#e8e8ee", "PRIMARY": "#1C6758", "BG": "#f7f8fa", "CARD_BG": "#ffffff", "TEXT": "#222", "MUTED": "#777" }
CARD_STYLE = f"background-color:{THEME['CARD_BG']}; border:1px solid {THEME['BORDER']}; border-radius:12px; padding:16px;"

st.markdown(f"""
<style>
html, body, [data-testid="stAppViewContainer"] {{ background: {THEME['BG']}; color: {THEME['TEXT']}; }}
.block-container {{ padding-top: 2.4rem; padding-bottom: 1.6rem; }}
[data-testid="stAppViewContainer"] .main .block-container {{ max-width: 1050px; margin: 0 auto; padding: 0 12px; }}
.stTabs [role="tablist"] {{ display:flex !important; gap:12px !important; flex-wrap:wrap !important; margin:8px 0 24px !important; border-bottom:none !important; }}
.stTabs button[role="tab"] {{ border:1px solid {THEME['BORDER']} !important; border-radius:12px !important; background:#fff !important; padding:10px 14px !important; box-shadow:0 1px 6px rgba(0,0,0,0.04) !important; }}
.stTabs button[role="tab"][aria-selected="true"] {{ border-color:{THEME['PRIMARY']} !important; color:{THEME['PRIMARY']} !important; box-shadow:0 6px 16px rgba(28,103,88,0.18) !important; font-weight:700; }}
</style>
""", unsafe_allow_html=True)

def v_spacer(height: int):
    st.markdown(f"<div style='height:{height}px'></div>", unsafe_allow_html=True)

KST = ZoneInfo("Asia/Seoul")
def now_kst_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str: return datetime.now(KST).strftime(fmt)

def display_feedback():
    if "success_message" in st.session_state and st.session_state.success_message:
        st.success(st.session_state.success_message)
        st.session_state.success_message = ""

# =============================================================================
# 1) Users 로더 (기존과 동일)
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
    if not cleaned: st.error("로그인 계정을 찾을 수 없습니다. Secrets 의 [users] 구조를 확인하세요."); st.stop()
    return cleaned

USERS = load_users_from_secrets()

# =============================================================================
# 2) [수정] 시트/스키마 정의
# =============================================================================
SHEET_NAME_STORES = "지점마스터"
SHEET_NAME_MASTER = "상품마스터"
SHEET_NAME_ORDERS = "발주"
SHEET_NAME_LOG = "변경로그"

# [수정] 상품마스터 시트 컬럼 정의
MASTER_COLUMNS = ["품목코드", "품목명", "품목규격", "분류", "단위", "판매단가", "과세구분", "활성"]
# [수정] 발주 시트 컬럼 정의 (금액 세분화)
ORDERS_COLUMNS = ["주문일시", "발주번호", "지점ID", "지점명", "납품요청일", "품목코드", "품목명", "단위", "수량", "판매단가", "공급가액", "세액", "합계금액", "비고", "상태", "처리일시", "처리자"]
# [수정] 장바구니 컬럼 정의
CART_COLUMNS = ["품목코드", "품목명", "단위", "판매단가", "수량", "합계금액"]
# [신설] 변경로그 시트 컬럼 정의
LOG_COLUMNS = ["변경일시", "변경자", "대상시트", "품목코드", "변경항목", "이전값", "새로운값"]

# =============================================================================
# 3) Google Sheets 연결 (기존과 동일)
# =============================================================================
@st.cache_resource(show_spinner=False)
def get_gs_client():
    google = st.secrets.get("google", {})
    creds_info = dict(google)
    if "\\n" in str(creds_info.get("private_key", "")):
        creds_info["private_key"] = str(creds_info["private_key"]).replace("\\n", "\n")
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def open_spreadsheet():
    key = str(st.secrets.get("google", {}).get("SPREADSHEET_KEY", "")).strip()
    if not key: st.error("Secrets 에 SPREADSHEET_KEY가 없습니다."); st.stop()
    try: return get_gs_client().open_by_key(key)
    except Exception as e: st.error(f"스프레드시트 열기 실패: {e}"); st.stop()

# =============================================================================
# 4) [수정] 데이터 I/O 함수
# =============================================================================
# [신설] 지점마스터 로드 함수
@st.cache_data(ttl=3600)
def load_store_info_df() -> pd.DataFrame:
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_STORES)
        df = pd.DataFrame(ws.get_all_records())
        # 필수 컬럼 존재 여부 확인 및 기본값 처리
        for col in ["지점ID", "상호명", "사업자등록번호", "대표자명", "사업장주소"]:
            if col not in df.columns:
                df[col] = ""
        return df
    except gspread.WorksheetNotFound:
        st.error(f"'{SHEET_NAME_STORES}' 시트를 찾을 수 없습니다.")
        return pd.DataFrame()

# [수정] 상품마스터 로드 함수
@st.cache_data(ttl=180)
def load_master_df() -> pd.DataFrame:
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_MASTER)
        df = pd.DataFrame(ws.get_all_records())
        for c in MASTER_COLUMNS:
            if c not in df.columns:
                df[c] = (0 if c == "판매단가" else (True if c == "활성" else ""))
        
        mask = df["활성"].astype(str).str.lower().isin(["1", "true", "y", "yes"])
        df = df[mask | df["활성"].isna()]
        df["판매단가"] = pd.to_numeric(df["판매단가"], errors="coerce").fillna(0).astype(int)
        return df
    except gspread.WorksheetNotFound:
        st.error(f"'{SHEET_NAME_MASTER}' 시트를 찾을 수 없습니다.")
        return pd.DataFrame()

# [수정] 상품마스터 쓰기 함수 (변경로그 연동)
def write_master_df(df: pd.DataFrame, original_df: pd.DataFrame) -> bool:
    # --- 변경사항 비교 및 로그 생성 ---
    log_entries = []
    user_name = st.session_state.auth["name"]
    timestamp = now_kst_str()

    # 인덱스를 품목코드로 설정하여 비교 용이하게 만듦
    df = df.set_index("품목코드")
    original_df = original_df.set_index("품목코드")

    # 추가된 품목
    new_items = df.index.difference(original_df.index)
    for code in new_items:
        log_entries.append({"변경일시": timestamp, "변경자": user_name, "대상시트": SHEET_NAME_MASTER, "품목코드": code, "변경항목": "품목추가", "이전값": "", "새로운값": df.loc[code].to_json()})
    
    # 삭제된 품목
    deleted_items = original_df.index.difference(df.index)
    for code in deleted_items:
        log_entries.append({"변경일시": timestamp, "변경자": user_name, "대상시트": SHEET_NAME_MASTER, "품목코드": code, "변경항목": "품목삭제", "이전값": original_df.loc[code].to_json(), "새로운값": ""})

    # 수정된 품목
    common_items = df.index.intersection(original_df.index)
    for code in common_items:
        diff = df.loc[code] != original_df.loc[code]
        if diff.any():
            changed_cols = diff[diff].index.tolist()
            for col in changed_cols:
                log_entries.append({"변경일시": timestamp, "변경자": user_name, "대상시트": SHEET_NAME_MASTER, "품목코드": code, "변경항목": col, "이전값": str(original_df.loc[code, col]), "새로운값": str(df.loc[code, col])})
    
    if log_entries:
        append_change_log(log_entries)

    # --- 시트 업데이트 ---
    try:
        df_to_write = df.reset_index()
        sh = open_spreadsheet()
        ws = sh.worksheet(SHEET_NAME_MASTER)
        ws.clear()
        ws.update("A1", [MASTER_COLUMNS] + df_to_write[MASTER_COLUMNS].fillna("").values.tolist(), value_input_option='USER_ENTERED')
        load_master_df.clear()
        return True
    except Exception as e:
        st.error(f"상품마스터 저장 실패: {e}")
        return False


# [수정] 발주 시트 로드 함수
@st.cache_data(ttl=60)
def load_orders_df() -> pd.DataFrame:
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_ORDERS)
        df = pd.DataFrame(ws.get_all_records())
        for c in ORDERS_COLUMNS:
            if c not in df.columns: df[c] = ""
        
        money_cols = ["수량", "판매단가", "공급가액", "세액", "합계금액"]
        for c in money_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        
        return df[ORDERS_COLUMNS].copy()
    except gspread.WorksheetNotFound:
        st.error(f"'{SHEET_NAME_ORDERS}' 시트를 찾을 수 없습니다.")
        return pd.DataFrame(columns=ORDERS_COLUMNS)

def write_orders_df(df: pd.DataFrame) -> bool:
    try:
        sh = open_spreadsheet(); ws = sh.worksheet(SHEET_NAME_ORDERS)
        ws.clear()
        ws.update("A1", [ORDERS_COLUMNS] + df[ORDERS_COLUMNS].fillna("").values.tolist(), value_input_option='USER_ENTERED')
        load_orders_df.clear()
        return True
    except Exception as e:
        st.error(f"발주 저장 실패: {e}")
        return False

# [수정] 발주 추가 함수
def append_orders(rows: List[Dict[str, Any]]) -> bool:
    if not rows: return True
    try:
        sh = open_spreadsheet(); ws = sh.worksheet(SHEET_NAME_ORDERS)
        values_to_add = [[r.get(col, "") for col in ORDERS_COLUMNS] for r in rows]
        ws.append_rows(values_to_add, value_input_option='USER_ENTERED')
        load_orders_df.clear()
        return True
    except Exception as e:
        st.error(f"발주 추가 실패: {e}")
        return False

# [신설] 변경로그 추가 함수
def append_change_log(log_entries: List[Dict[str, Any]]):
    if not log_entries: return True
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_LOG)
        values_to_add = [[entry.get(col, "") for col in LOG_COLUMNS] for entry in log_entries]
        ws.append_rows(values_to_add, value_input_option='USER_ENTERED')
        return True
    except gspread.WorksheetNotFound:
        st.warning(f"'{SHEET_NAME_LOG}' 시트가 없어 로그를 기록하지 못했습니다.")
        return False
    except Exception as e:
        st.error(f"변경로그 기록 실패: {e}")
        return False

# [수정] 주문 상태 변경 함수 (삭제 시 로그 기록 추가)
def update_order_status(selected_ids: List[str], new_status: str, handler: str) -> bool:
    df = load_orders_df()
    now = now_kst_str()
    mask = df["발주번호"].astype(str).isin([str(x) for x in selected_ids])
    
    # 주문 삭제 시 로그 기록
    if new_status.lower() in ["deleted", "cancelled", "삭제", "주문취소"]:
        log_entries = []
        for order_id in selected_ids:
            log_entries.append({
                "변경일시": now, "변경자": handler, "대상시트": SHEET_NAME_ORDERS,
                "품목코드": f"발주번호: {order_id}", "변경항목": "주문상태",
                "이전값": "접수", "새로운값": "삭제"
            })
        append_change_log(log_entries)
        # 실제 데이터프레임에서 행 삭제
        df_updated = df[~mask]
    else:
        df.loc[mask, "상태"] = new_status
        df.loc[mask, "처리일시"] = now
        df.loc[mask, "처리자"] = handler
        df_updated = df

    return write_orders_df(df_updated)

# =============================================================================
# 5) 로그인 (기존과 동일)
# =============================================================================
def require_login():
    if st.session_state.get("auth", {}).get("login"): return True
    st.markdown('<div style="text-align:center; font-size:42px; font-weight:800; margin:16px 0 12px;">식자재 발주 시스템</div>', unsafe_allow_html=True)
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
    if stored_hash: return hashlib.sha256(input_pw.encode()).hexdigest() == stored_hash.strip().lower()
    return str(input_pw) == str(fallback_plain) if fallback_plain is not None else False

def _find_account(uid_or_name: str):
    s_lower = str(uid_or_name or "").strip().lower()
    if not s_lower: return None, None
    for uid, acct in USERS.items():
        if uid.lower() == s_lower or acct.get("name", "").lower() == s_lower: return uid, acct
    return None, None

# =============================================================================
# 6) 유틸 함수
# =============================================================================
def make_order_id(store_id: str) -> str: return f"{datetime.now(KST):%Y%m%d%H%M%S}{store_id}"

# [신설] 거래명세서 등 문서 생성 함수 (기존 함수를 템플릿으로 활용)
def make_document_excel(df_doc: pd.DataFrame, doc_type: str, store_info: pd.Series) -> BytesIO:
    # 이 함수는 실제 거래명세서/세금계산서 양식에 맞게 복잡하게 구성되어야 합니다.
    # 여기서는 간단한 예시 형태로 구성합니다.
    buf = BytesIO()
    workbook = xlsxwriter.Workbook(buf, {'in_memory': True, 'default_date_format': 'yyyy-mm-dd'})
    ws = workbook.add_worksheet(doc_type)

    # 서식 정의
    fmt_h1 = workbook.add_format({"bold": True, "font_size": 20, "align": "center"})
    fmt_money = workbook.add_format({"num_format": "#,##0"})

    # 문서 제목
    ws.merge_range("A1:G1", f"산카쿠 {doc_type}", fmt_h1)

    # 공급받는자 정보 (지점마스터에서 가져옴)
    ws.write("A3", "상호:")
    ws.write("B3", store_info.get("상호명", ""))
    ws.write("A4", "사업자번호:")
    ws.write("B4", store_info.get("사업자등록번호", ""))
    ws.write("A5", "주소:")
    ws.write("B5", store_info.get("사업장주소", ""))
    ws.write("A6", "대표:")
    ws.write("B6", store_info.get("대표자명", ""))

    # 품목 내역
    headers = ["품목명", "규격", "단위", "수량", "단가", "공급가액", "세액"]
    for i, header in enumerate(headers):
        ws.write(8, i, header)
    
    row_num = 9
    for _, item in df_doc.iterrows():
        ws.write(row_num, 0, item["품목명"])
        # 규격은 상품마스터에서 가져와야 함 (여기서는 생략)
        ws.write(row_num, 2, item["단위"])
        ws.write(row_num, 3, item["수량"], fmt_money)
        ws.write(row_num, 4, item["판매단가"], fmt_money)
        ws.write(row_num, 5, item["공급가액"], fmt_money)
        ws.write(row_num, 6, item["세액"], fmt_money)
        row_num += 1

    # 합계
    total_supply = df_doc["공급가액"].sum()
    total_tax = df_doc["세액"].sum()
    total_amount = df_doc["합계금액"].sum()
    ws.write(row_num + 1, 4, "공급가액 합계")
    ws.write(row_num + 1, 5, total_supply, fmt_money)
    ws.write(row_num + 2, 4, "세액 합계")
    ws.write(row_num + 2, 5, total_tax, fmt_money)
    ws.write(row_num + 3, 4, "총 합계")
    ws.write(row_num + 3, 5, total_amount, fmt_money)

    workbook.close()
    buf.seek(0)
    return buf

# =============================================================================
# 7) [수정] 장바구니 유틸
# =============================================================================
def init_session_state():
    defaults = { "cart": pd.DataFrame(columns=CART_COLUMNS), "store_editor_ver": 0, "cart_selected_codes": [], "store_selected_orders": [], "admin_pending_selection": [], "admin_shipped_selection": [], "success_message": "" }
    for key, value in defaults.items():
        if key not in st.session_state: st.session_state[key] = value

def coerce_cart_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in CART_COLUMNS:
        if col not in out.columns: out[col] = 0 if col in ["판매단가", "수량", "합계금액"] else ""
    
    out["수량"] = pd.to_numeric(out["수량"], errors="coerce").fillna(0).astype(int)
    out["판매단가"] = pd.to_numeric(out["판매단가"], errors="coerce").fillna(0).astype(int)
    out["합계금액"] = out["판매단가"] * out["수량"]
    return out[CART_COLUMNS]

def add_to_cart(rows_df: pd.DataFrame):
    add = rows_df[rows_df["수량"] > 0].copy()
    if add.empty: return
    
    # 합계금액 계산 추가
    add["합계금액"] = add["판매단가"] * add["수량"]
    
    cart = st.session_state.cart.copy()
    
    merged = pd.concat([cart, add]).groupby("품목코드", as_index=False).agg({
        "품목명": "last", "단위": "last", "판매단가": "last", "수량": "sum"
    })
    merged["합계금액"] = merged["판매단가"] * merged["수량"]
    st.session_state.cart = merged[CART_COLUMNS]

def remove_from_cart(codes: list[str]):
    if codes: st.session_state.cart = st.session_state.cart[~st.session_state.cart["품목코드"].isin(codes)]

# =============================================================================
# 8) [수정] 지점(Store) 페이지
# =============================================================================
def page_store_register_confirm(master_df: pd.DataFrame):
    st.subheader("🛒 발주 요청")
    v_spacer(10)
    # --- 1. 납품 요청 정보 ---
    with st.container(border=True):
        st.markdown("##### 🗓️ 납품 요청 정보")
        today, max_day = date.today(), date.today() + timedelta(days=7)
        c1, c2 = st.columns([1, 1.2])
        quick = c1.radio("납품 선택", ["오늘", "내일", "직접선택"], horizontal=True, label_visibility="collapsed")
        if quick == "오늘": 납품요청일 = today
        elif quick == "내일": 납품요청일 = today + timedelta(days=1)
        else: 납품요청일 = c2.date_input("납품 요청일", value=today, min_value=today, max_value=max_day, label_visibility="collapsed")
        memo = st.text_area("요청 사항(선택)", height=80, placeholder="예) 입고 시 얼음팩 추가 부탁드립니다.")
    
    v_spacer(16)
    # --- 2. 발주 수량 입력 ---
    with st.container(border=True):
        st.markdown("##### 🧾 발주 수량 입력")
        l, r = st.columns([2, 1])
        keyword = l.text_input("품목 검색(이름/코드)", placeholder="오이, P001 등")
        cat_opt = ["(전체)"] + sorted(master_df["분류"].dropna().unique().tolist())
        cat_sel = r.selectbox("분류(선택)", cat_opt)
        
        df_view = master_df.copy()
        if keyword: df_view = df_view[df_view.apply(lambda row: keyword.strip().lower() in str(row["품목명"]).lower() or keyword.strip().lower() in str(row["품목코드"]).lower(), axis=1)]
        if cat_sel != "(전체)": df_view = df_view[df_view["분류"] == cat_sel]

        with st.form(key="add_to_cart_form"):
            df_edit = df_view[["품목코드", "품목명", "단위", "판매단가"]].copy()
            df_edit["수량"] = ""
            edited_disp = st.data_editor(df_edit, key=f"editor_v{st.session_state.store_editor_ver}", hide_index=True, disabled=["품목코드", "품목명", "단위", "판매단가"], use_container_width=True,
                column_config={"판매단가": st.column_config.NumberColumn(format="%,d원"), "수량": st.column_config.NumberColumn(min_value=0)})
            
            if st.form_submit_button("장바구니 추가", use_container_width=True, type="primary"):
                items_to_add = coerce_cart_df(edited_disp)
                if not items_to_add[items_to_add["수량"] > 0].empty:
                    add_to_cart(items_to_add)
                    st.session_state.store_editor_ver += 1
                st.rerun()

    v_spacer(16)
    # --- 3. 장바구니 ---
    with st.container(border=True):
        st.markdown("##### 🧺 장바구니")
        cart = st.session_state.cart
        if not cart.empty:
            edited_cart = st.data_editor(cart, key="cart_editor", hide_index=True, disabled=["품목코드", "품목명", "단위", "합계금액"],
                column_config={"판매단가": st.column_config.NumberColumn(format="%,d원"), "합계금액": st.column_config.NumberColumn(format="%,d원")})
            
            st.session_state.cart = coerce_cart_df(edited_cart)
            
            # 장바구니 액션 버튼 (선택 기능 제거, 단순화)
            if st.button("장바구니 비우기", use_container_width=True):
                st.session_state.cart = pd.DataFrame(columns=CART_COLUMNS)
                st.rerun()
        else:
            st.info("장바구니가 비어 있습니다.")

    v_spacer(16)
    # --- 4. 발주 제출 ---
    with st.form("submit_form"):
        cart_now = st.session_state.cart
        total_amount_sum = cart_now['합계금액'].sum()
        st.markdown(f"**최종 확인:** 총 {len(cart_now)}개 품목, 합계 {total_amount_sum:,}원")
        confirm = st.checkbox("위 내용으로 발주를 제출합니다.")

        if st.form_submit_button("📦 발주 제출", type="primary", use_container_width=True, disabled=cart_now.empty):
            if not confirm:
                st.warning("제출 확인 체크박스를 선택해주세요.")
                st.stop()
            
            # [핵심 로직] 발주 데이터 생성 시 공급가액/세액 계산
            user = st.session_state.auth
            order_id = make_order_id(user["user_id"])
            
            # 상품마스터와 장바구니 병합하여 '과세구분' 정보 가져오기
            cart_with_master = pd.merge(cart_now, master_df[['품목코드', '과세구분']], on='품목코드', how='left')

            rows = []
            for _, r in cart_with_master.iterrows():
                total_amount = r['합계금액']
                tax_type = r['과세구분']
                
                if tax_type == '과세':
                    supply_price = math.ceil(total_amount / 1.1)
                    tax_amount = total_amount - supply_price
                else: # 면세
                    supply_price = total_amount
                    tax_amount = 0
                
                order_item = {
                    "주문일시": now_kst_str(), "발주번호": order_id, "지점ID": user["user_id"], 
                    "지점명": user["name"], "납품요청일": f"{납품요청일:%Y-%m-%d}", 
                    "품목코드": r["품목코드"], "품목명": r["품목명"], "단위": r["단위"], "수량": r["수량"],
                    "판매단가": r["판매단가"], "공급가액": supply_price, "세액": tax_amount, 
                    "합계금액": total_amount, "비고": memo, "상태": "접수", 
                    "처리일시": "", "처리자": ""
                }
                rows.append(order_item)
            
            if append_orders(rows):
                st.session_state.success_message = "발주가 성공적으로 제출되었습니다."
                st.session_state.cart = pd.DataFrame(columns=CART_COLUMNS)
                st.rerun()
            else:
                st.error("발주 제출 중 오류가 발생했습니다.")


def page_store_orders_change():
    st.subheader("🧾 발주 조회·수정")
    display_feedback()
    df_all, user = load_orders_df(), st.session_state.auth
    df_user = df_all[df_all["지점ID"] == user["user_id"]]
    if df_user.empty: st.info("발주 데이터가 없습니다."); return
    
    # 발주 리스트 (합계금액으로 집계)
    orders = df_user.groupby("발주번호").agg(
        주문일시=("주문일시", "first"), 
        건수=("품목코드", "count"), 
        합계금액=("합계금액", "sum"), 
        상태=("상태", "first")
    ).reset_index().sort_values("주문일시", ascending=False)
    
    st.dataframe(orders, hide_index=True, use_container_width=True,
                 column_config={"합계금액": st.column_config.NumberColumn(format="%,d원")})

    # 삭제 기능 (접수 상태인 것만 선택 가능)
    pending_orders = orders[orders["상태"] == "접수"]["발주번호"].tolist()
    order_to_delete = st.selectbox("삭제할 발주번호 선택 (접수 상태만 가능)", [""] + pending_orders)

    if st.button("선택 발주 삭제", disabled=not order_to_delete):
        if update_order_status([order_to_delete], "삭제", user["name"]):
            st.session_state.success_message = f"발주번호 {order_to_delete}가 삭제되었습니다."
            st.rerun()


def page_store_documents(store_info_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")
    user = st.session_state.auth
    df = load_orders_df()[load_orders_df()["지점ID"] == user["user_id"]]
    if df.empty: st.info("발주 데이터가 없습니다."); return

    c1, c2, c3 = st.columns(3)
    dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=30))
    dt_to = c2.date_input("조회 종료일", date.today())
    doc_type = c3.selectbox("문서 종류", ["거래명세서", "세금계산서 (양식)"])

    mask = (pd.to_datetime(df["납품요청일"]).dt.date >= dt_from) & (pd.to_datetime(df["납품요청일"]).dt.date <= dt_to)
    dfv = df[mask].copy()

    if dfv.empty:
        st.warning("해당 기간에 조회된 데이터가 없습니다.")
        st.stop()

    st.dataframe(dfv, use_container_width=True, hide_index=True)

    if st.button(f"{doc_type} 다운로드", use_container_width=True, type="primary"):
        store_info = store_info_df[store_info_df["지점ID"] == user["user_id"]].iloc[0]
        buf = make_document_excel(dfv, doc_type, store_info)
        st.download_button("다운로드 시작", data=buf, 
                            file_name=f"{doc_type}_{user['name']}_{dt_from}~{dt_to}.xlsx", 
                            mime="application/vnd.ms-excel")

def page_store_master_view(master_df: pd.DataFrame):
    st.subheader("🏷️ 품목 가격 조회")
    v_spacer(10)
    st.dataframe(master_df[["품목코드", "품목명", "품목규격", "분류", "단위", "판매단가"]], 
                 use_container_width=True, hide_index=True, 
                 column_config={"판매단가": st.column_config.NumberColumn(format="%,d원")})

# =============================================================================
# 9) [수정] 관리자(Admin) 페이지
# =============================================================================
def page_admin_unified_management():
    st.subheader("📋 발주요청 조회·수정")
    display_feedback()
    df_all = load_orders_df()
    if df_all.empty: st.info("발주 데이터가 없습니다."); return

    # --- 필터 ---
    c1, c2, c3 = st.columns([1, 1, 2])
    dt_from = c1.date_input("시작일", date.today() - timedelta(days=7), key="admin_mng_from")
    dt_to = c2.date_input("종료일", date.today(), key="admin_mng_to")
    stores = ["(전체)"] + sorted(df_all["지점명"].dropna().unique().tolist())
    store = c3.selectbox("지점", stores, key="admin_mng_store")
    
    df = df_all[(pd.to_datetime(df_all["납품요청일"]).dt.date >= dt_from) & (pd.to_datetime(df_all["납품요청일"]).dt.date <= dt_to)]
    if store != "(전체)": df = df[df["지점명"] == store]

    orders = df.groupby("발주번호").agg(
        주문일시=("주문일시", "first"), 지점명=("지점명", "first"), 건수=("품목코드", "count"),
        합계금액=("합계금액", "sum"), 상태=("상태", "first")
    ).reset_index().sort_values("주문일시", ascending=False)
    
    pending = orders[orders["상태"] == "접수"]
    shipped = orders[orders["상태"] == "출고완료"]

    # --- 탭 화면 ---
    tab1, tab2 = st.tabs([f"📦 발주 요청 접수 ({len(pending)}건)", f"✅ 출고 완료 ({len(shipped)}건)"])
    
    with tab1:
        st.dataframe(pending, use_container_width=True, hide_index=True,
                     column_config={"합계금액": st.column_config.NumberColumn(format="%,d원")})
        ids_to_ship = st.multiselect("출고 처리할 발주번호를 선택하세요.", pending["발주번호"].tolist())
        if st.button("✅ 선택 발주 출고", disabled=not ids_to_ship):
            if update_order_status(ids_to_ship, "출고완료", st.session_state.auth["name"]):
                st.session_state.success_message = f"{len(ids_to_ship)}건이 출고 처리되었습니다."
                st.rerun()

    with tab2:
        st.dataframe(shipped, use_container_width=True, hide_index=True,
                     column_config={"합계금액": st.column_config.NumberColumn(format="%,d원")})
        ids_to_revert = st.multiselect("접수 상태로 변경할 발주번호를 선택하세요.", shipped["발주번호"].tolist())
        if st.button("↩️ 접수 상태로 변경", disabled=not ids_to_revert):
            if update_order_status(ids_to_revert, "접수", st.session_state.auth["name"]):
                st.session_state.success_message = f"{len(ids_to_revert)}건이 접수 상태로 변경되었습니다."
                st.rerun()

def page_admin_documents(store_info_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")
    user = st.session_state.auth
    df = load_orders_df()
    if df.empty: st.info("발주 데이터가 없습니다."); return

    # 필터
    c1, c2, c3, c4 = st.columns(4)
    dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=30))
    dt_to = c2.date_input("조회 종료일", date.today())
    stores = sorted(df["지점명"].dropna().unique().tolist())
    store_sel = c3.selectbox("지점 선택", stores)
    doc_type = c4.selectbox("문서 종류", ["거래명세서", "세금계산서 (양식)"])

    mask = (pd.to_datetime(df["납품요청일"]).dt.date >= dt_from) & \
           (pd.to_datetime(df["납품요청일"]).dt.date <= dt_to) & \
           (df["지점명"] == store_sel)
    dfv = df[mask].copy()

    if dfv.empty:
        st.warning(f"{store_sel}의 해당 기간에 조회된 데이터가 없습니다.")
        st.stop()

    st.dataframe(dfv, use_container_width=True, hide_index=True)

    if st.button(f"'{store_sel}' {doc_type} 다운로드", use_container_width=True, type="primary"):
        store_id = dfv.iloc[0]["지점ID"]
        store_info = store_info_df[store_info_df["지점ID"] == store_id].iloc[0]
        buf = make_document_excel(dfv, doc_type, store_info)
        st.download_button("다운로드 시작", data=buf, 
                            file_name=f"{doc_type}_{store_sel}_{dt_from}~{dt_to}.xlsx", 
                            mime="application/vnd.ms-excel")


def page_admin_items_price(master_df: pd.DataFrame):
    st.subheader("🏷️ 품목 가격 설정")
    st.caption("가격을 수정하거나 품목을 추가/삭제한 후 '변경사항 저장' 버튼을 누르세요. 모든 변경 내역은 로그에 기록됩니다.")
    
    # 원본 복사 (비교용)
    original_df = master_df.copy()

    with st.form("master_edit_form"):
        edited = st.data_editor(master_df.assign(삭제=False), hide_index=True, num_rows="dynamic", use_container_width=True,
            column_config={"판매단가": st.column_config.NumberColumn(format="%,d원")})
        
        if st.form_submit_button("변경사항 저장", type="primary", use_container_width=True):
            edited['삭제'] = edited['삭제'].fillna(False).astype(bool)
            final_df = edited[~edited["삭제"]].drop(columns=["삭제"])
            
            # 원본 df와 함께 전달하여 차이점 로깅
            if write_master_df(final_df, original_df):
                st.session_state.success_message = "상품마스터가 저장되었습니다."; 
                st.rerun()

# [신설] 관리자 매출 조회 페이지
def page_admin_sales_inquiry():
    st.subheader("📈 매출 조회")
    df_orders = load_orders_df()
    if df_orders.empty:
        st.info("매출 데이터가 없습니다."); return

    # --- 필터 ---
    c1, c2, c3 = st.columns(3)
    dt_from = c1.date_input("조회 시작일", date.today().replace(day=1))
    dt_to = c2.date_input("조회 종료일", date.today())
    stores = ["(전체 통합)"] + sorted(df_orders["지점명"].dropna().unique().tolist())
    store = c3.selectbox("조회 지점", stores)

    mask = (pd.to_datetime(df_orders["주문일시"]).dt.date >= dt_from) & (pd.to_datetime(df_orders["주문일시"]).dt.date <= dt_to)
    if store != "(전체 통합)":
        mask &= (df_orders["지점명"] == store)
    
    df_sales = df_orders[mask].copy()

    if df_sales.empty:
        st.warning("해당 조건의 매출 데이터가 없습니다."); st.stop()

    # --- 현황판 ---
    total_sales = df_sales["합계금액"].sum()
    total_supply = df_sales["공급가액"].sum()
    total_tax = df_sales["세액"].sum()

    m1, m2, m3 = st.columns(3)
    m1.metric("총 매출 (VAT 포함)", f"{total_sales:,}원")
    m2.metric("공급가액", f"{total_supply:,}원")
    m3.metric("부가세액", f"{total_tax:,}원")
    
    st.divider()

    # --- 시각화 ---
    df_sales["일자"] = pd.to_datetime(df_sales["주문일시"]).dt.date

    # 일별 매출 추이
    st.markdown("##### 📅 일별 매출 추이")
    daily_sales = df_sales.groupby("일자")["합계금액"].sum()
    st.bar_chart(daily_sales)

    # 품목별 매출 순위
    st.markdown("##### 🍔 품목별 매출 순위 (Top 10)")
    item_sales = df_sales.groupby("품목명")["합계금액"].sum().nlargest(10)
    st.dataframe(item_sales.reset_index(), use_container_width=True, hide_index=True,
                 column_config={"합계금액": st.column_config.NumberColumn(format="%,d원")})

# =============================================================================
# 10) [수정] 라우팅
# =============================================================================
if __name__ == "__main__":
    if not require_login(): st.stop()
    
    init_session_state()
    st.title("📦 식자재 발주 시스템")
    display_feedback()
    user = st.session_state.auth
    
    # 공통 데이터 로딩
    master_df = load_master_df()
    store_info_df = load_store_info_df()

    if user["role"] == "admin":
        tabs = st.tabs(["📋 발주요청 조회·수정", "📈 매출 조회", "📑 증빙서류 다운로드", "🏷️ 품목 가격 설정"])
        with tabs[0]: page_admin_unified_management()
        with tabs[1]: page_admin_sales_inquiry()
        with tabs[2]: page_admin_documents(store_info_df)
        with tabs[3]: page_admin_items_price(master_df)
    else: # store
        tabs = st.tabs(["🛒 발주 요청", "🧾 발주 조회·수정", "📑 증빙서류 다운로드", "🏷️ 품목 가격 조회"])
        with tabs[0]: page_store_register_confirm(master_df)
        with tabs[1]: page_store_orders_change()
        with tabs[2]: page_store_documents(store_info_df)
        with tabs[3]: page_store_master_view(master_df)
