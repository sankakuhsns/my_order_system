# -*- coding: utf-8 -*-
# =============================================================================
# Streamlit 식자재 발주 시스템 (무료 지향 · Google Sheets 연동)
# - 역할: 지점(발주), 본사/공장(조회·출고처리)
# - 저장: Google Sheets (미연결 시 로컬 CSV 백업)
# - 인증: st.secrets["users"] (테스트용 간단 비번)
# - 상품마스터: "상품마스터" 시트 (선택 컬럼 지원: 단가/최소수량/최대수량/묶음단위/활성)
# - 발주기록:  "발주" 시트
# =============================================================================

import os
from pathlib import Path
import uuid
from datetime import datetime, date, timedelta
from typing import Dict, Any, List

import pandas as pd
import streamlit as st

# ---- Google Sheets 연동 (선택)
try:
    import gspread
    from google.oauth2 import service_account
    GDRIVE_AVAILABLE = True
except Exception:
    GDRIVE_AVAILABLE = False

# =============================================================================
# 0) 페이지/테마
# =============================================================================
st.set_page_config(page_title="발주 시스템", page_icon="📦", layout="wide")

THEME = {
    "BORDER": "#e8e8e8",
    "CARD": "background-color:#ffffff;border:1px solid #e8e8e8;border-radius:12px;padding:16px;",
}
COMMON_CSS = f"""
<style>
    .small {{font-size: 12px; color: #777;}}
    .tag {{display:inline-block; padding:2px 8px; border-radius:999px; border:1px solid #ddd; margin-right:6px}}
    .ok {{background:#ecfff3; border-color:#cde9d7}}
    .warn {{background:#fff7e6; border-color:#ffe1a8}}
    .danger {{background:#fff0f0; border-color:#ffd6d6}}
    .card {{ {THEME["CARD"]} }}
</style>
"""
st.markdown(COMMON_CSS, unsafe_allow_html=True)

# =============================================================================
# 1) 상수/설정
# =============================================================================
SHEET_NAME_MASTER = "상품마스터"
SHEET_NAME_ORDERS = "발주"
ORDER_STATUSES = ["접수", "출고완료"]

LOCAL_BACKUP_DIR = Path("local_backup")
LOCAL_BACKUP_DIR.mkdir(exist_ok=True)
LOCAL_BACKUP_ORDERS = str(LOCAL_BACKUP_DIR / "orders_backup.csv")

# =============================================================================
# 2) 사용자 로드 (st.secrets["users"])
# =============================================================================
def load_users_from_secrets() -> pd.DataFrame:
    rows = []
    users = st.secrets.get("users", {})
    try:
        for uid, payload in users.items():
            if isinstance(payload, dict):
                rows.append({
                    "user_id": uid,
                    "password": payload.get("password", ""),
                    "name": payload.get("name", uid),
                    "role": payload.get("role", "store"),
                })
    except Exception:
        pass
    return pd.DataFrame(rows)

USERS_DF = load_users_from_secrets()

# =============================================================================
# 3) Google Sheets 클라이언트/스프레드시트
# =============================================================================
@st.cache_resource(show_spinner=False)
def get_gs_client():
    if not GDRIVE_AVAILABLE:
        return None
    try:
        creds_dict = st.secrets.get("google", None)
        if not creds_dict:
            return None
        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ],
        )
        return gspread.authorize(creds)
    except Exception as e:
        st.warning(f"Google 인증 실패: {e}")
        return None

@st.cache_resource(show_spinner=False)
def get_spreadsheet_key() -> str:
    key = st.secrets.get("SPREADSHEET_KEY", "") or st.secrets.get("google", {}).get("SPREADSHEET_KEY", "")
    if isinstance(key, str):
        return key.strip()
    return str(key).strip()

@st.cache_resource(show_spinner=False)
def open_spreadsheet():
    gc = get_gs_client()
    key = get_spreadsheet_key()
    if not gc or not key:
        return None
    try:
        return gc.open_by_key(key)
    except Exception as e:
        st.error(f"스프레드시트 열기 실패: {e}")
        return None

# =============================================================================
# 4) 데이터 I/O
# =============================================================================
@st.cache_data(ttl=120)
def load_master_df() -> pd.DataFrame:
    """
    상품마스터 로드
    필수: 품목코드, 품목명, 단위
    선택: 단가, 최소수량, 최대수량, 묶음단위, 활성, 기본리드타임, 안전재고
    """
    sh = open_spreadsheet()
    if sh:
        try:
            ws = sh.worksheet(SHEET_NAME_MASTER)
            df = pd.DataFrame(ws.get_all_records())
        except Exception as e:
            st.warning(f"상품마스터 로딩 실패(시트): {e}")
            df = pd.DataFrame()
    else:
        df = pd.DataFrame()

    if df.empty:
        # 샘플
        df = pd.DataFrame([
            {"품목코드":"P001","품목명":"오이","단위":"EA","단가":800,"최소수량":1,"최대수량":50,"묶음단위":1,"활성":True},
            {"품목코드":"P002","품목명":"대파","단위":"KG","단가":15600,"최소수량":1,"최대수량":30,"묶음단위":1,"활성":True},
            {"품목코드":"P003","품목명":"간장","단위":"L","단가":3500,"최소수량":1,"최대수량":100,"묶음단위":1,"활성":True},
        ])

    # 컬럼 보정/기본값
    for c in ["단가","최소수량","최대수량","묶음단위"]:
        if c not in df.columns: df[c] = None
    if "활성" not in df.columns: df["활성"] = True

    # 활성 필터
    df["활성_norm"] = df["활성"].astype(str).str.lower().isin(["1","true","y","yes"])
    df = df[df["활성_norm"] | df["활성"].isna()].drop(columns=["활성_norm"], errors="ignore")

    return df

@st.cache_data(ttl=60)
def load_orders_df() -> pd.DataFrame:
    sh = open_spreadsheet()
    if sh:
        try:
            ws = sh.worksheet(SHEET_NAME_ORDERS)
            df = pd.DataFrame(ws.get_all_records())
            return df
        except Exception as e:
            st.warning(f"발주 로딩 실패(시트): {e}")

    # 로컬 백업
    if os.path.exists(LOCAL_BACKUP_ORDERS):
        try:
            return pd.read_csv(LOCAL_BACKUP_ORDERS, encoding="utf-8-sig")
        except Exception:
            pass

    return pd.DataFrame(columns=[
        "주문일시","발주번호","지점ID","지점명","품목코드","품목명","단위","수량","비고","상태","처리일시","처리자"
    ])

def _ensure_orders_sheet_columns(ws):
    records = ws.get_all_values()
    if len(records) == 0:
        header = [
            "주문일시","발주번호","지점ID","지점명","품목코드","품목명","단위","수량","비고","상태","처리일시","처리자"
        ]
        ws.append_row(header)

def append_orders(rows: List[Dict[str, Any]]):
    sh = open_spreadsheet()
    if sh:
        try:
            try:
                ws = sh.worksheet(SHEET_NAME_ORDERS)
            except Exception:
                ws = sh.add_worksheet(title=SHEET_NAME_ORDERS, rows=1000, cols=20)
            _ensure_orders_sheet_columns(ws)
            for r in rows:
                ws.append_row([
                    r.get("주문일시",""), r.get("발주번호",""), r.get("지점ID",""), r.get("지점명",""),
                    r.get("품목코드",""), r.get("품목명",""), r.get("단위",""), r.get("수량",0),
                    r.get("비고",""), r.get("상태","접수"), r.get("처리일시",""), r.get("처리자","")
                ])
            load_orders_df.clear()  # 캐시 무효화
            return True
        except Exception as e:
            st.warning(f"시트 기록 실패: {e}")

    # 로컬 백업
    df_old = pd.DataFrame()
    if os.path.exists(LOCAL_BACKUP_ORDERS):
        try:
            df_old = pd.read_csv(LOCAL_BACKUP_ORDERS, encoding="utf-8-sig")
        except Exception:
            df_old = pd.DataFrame()
    df_new = pd.DataFrame(rows)
    df_all = pd.concat([df_old, df_new], ignore_index=True)
    os.makedirs(os.path.dirname(LOCAL_BACKUP_ORDERS), exist_ok=True)
    df_all.to_csv(LOCAL_BACKUP_ORDERS, index=False, encoding="utf-8-sig")
    load_orders_df.clear()
    return True

def update_order_status(selected_ids: List[str], new_status: str, handler: str):
    sh = open_spreadsheet()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if sh:
        try:
            ws = sh.worksheet(SHEET_NAME_ORDERS)
            data = ws.get_all_records()
            df = pd.DataFrame(data)
            if df.empty:
                st.warning("변경할 데이터가 없습니다.")
                return False
            mask = df["발주번호"].astype(str).isin([str(x) for x in selected_ids])
            df.loc[mask, "상태"] = new_status
            df.loc[mask, "처리일시"] = now
            df.loc[mask, "처리자"] = handler
            ws.clear()
            header = df.columns.tolist()
            ws.append_row(header)
            for _, row in df.iterrows():
                ws.append_row(list(row.values))
            load_orders_df.clear()
            return True
        except Exception as e:
            st.warning(f"상태 변경 실패(시트): {e}")

    # 로컬 백업 반영
    if os.path.exists(LOCAL_BACKUP_ORDERS):
        try:
            df = pd.read_csv(LOCAL_BACKUP_ORDERS, encoding="utf-8-sig")
            mask = df["발주번호"].astype(str).isin([str(x) for x in selected_ids])
            df.loc[mask, "상태"] = new_status
            df.loc[mask, "처리일시"] = now
            df.loc[mask, "처리자"] = handler
            df.to_csv(LOCAL_BACKUP_ORDERS, index=False, encoding="utf-8-sig")
            load_orders_df.clear()
            return True
        except Exception as e:
            st.error(f"상태 변경 실패(백업): {e}")
    return False

# =============================================================================
# 5) 유틸/인증
# =============================================================================
def make_order_id(store_id: str, seq: int) -> str:
    return f"{datetime.now():%Y%m%d-%H%M}-{store_id}-{seq:03d}"

def require_login():
    st.session_state.setdefault("auth", {})
    if st.session_state["auth"].get("login", False):
        return True

    st.header("🔐 로그인")
    if USERS_DF.empty:
        st.info("secrets에 사용자 계정을 등록하면 로그인 기능이 활성화됩니다. (지금은 게스트 접근)")
        if st.button("게스트로 계속"):
            st.session_state["auth"] = {"login": True, "user_id": "guest", "name": "게스트", "role": "admin"}
            st.rerun()
        return False

    user_ids = USERS_DF["user_id"].tolist()
    col1, col2 = st.columns([2, 1])
    with col1:
        uid = st.selectbox("아이디", user_ids)
    with col2:
        pwd = st.text_input("비밀번호", type="password")
    if st.button("로그인", use_container_width=True):
        row = USERS_DF[USERS_DF["user_id"] == uid].iloc[0]
        if str(pwd) == str(row["password"]):
            st.session_state["auth"] = {
                "login": True,
                "user_id": uid,
                "name": row["name"],
                "role": row["role"],
            }
            st.success(f"{row['name']}님 환영합니다!")
            st.rerun()
        else:
            st.error("아이디 또는 비밀번호가 올바르지 않습니다.")
    return False

# =============================================================================
# 6) 지점(발주) 화면
# =============================================================================
def page_store(master_df: pd.DataFrame):
    st.subheader("🛒 발주 등록")

    # ---- 검색/필터
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        keyword = st.text_input("품목 검색(이름/코드)")
    with col2:
        분류값 = st.selectbox("분류(선택)", ["(전체)"] + sorted(master_df.get("분류", pd.Series([])).dropna().unique().tolist())) \
            if "분류" in master_df.columns else "(전체)"
    with col3:
        pass

    df_view = master_df.copy()
    if keyword:
        q = keyword.strip().lower()
        df_view = df_view[df_view.apply(
            lambda r: q in str(r.get("품목명","")).lower() or q in str(r.get("품목코드","")).lower(), axis=1)]
    if "분류" in master_df.columns and 분류값 != "(전체)":
        df_view = df_view[df_view["분류"] == 분류값]

    preview_cols = [c for c in ["품목코드","품목명","단위","단가","최소수량","최대수량","묶음단위"] if c in df_view.columns]
    st.dataframe(df_view[preview_cols].reset_index(drop=True), use_container_width=True, height=240)

    st.markdown("---")
    st.markdown("**발주 수량 입력(표 일괄 편집)**")

    # 발주 편집용 테이블
    edit_cols = ["품목코드","품목명","단위"]
    if "단가" in master_df.columns: edit_cols += ["단가"]
    df_edit = df_view[edit_cols].copy()
    df_edit["수량"] = 0
    edited = st.data_editor(
        df_edit,
        column_config={"수량": st.column_config.NumberColumn(min_value=0, step=1)},
        use_container_width=True,
        num_rows="dynamic",
        key="order_editor_table",
    )

    # 합계/검증
    if "단가" in edited.columns:
        edited["금액"] = (pd.to_numeric(edited["단가"], errors="coerce").fillna(0) *
                         pd.to_numeric(edited["수량"], errors="coerce").fillna(0))
        total = int(edited["금액"].sum())
        st.markdown(f"<div class='card' style='margin-top:8px'>예상 합계: <b>{total:,} 원</b></div>", unsafe_allow_html=True)

    memo = st.text_input("요청 사항(선택)")
    confirm = st.checkbox("제출 전 입력 내용 확인했습니다.", value=False)

    def _validate_qty(row) -> List[str]:
        errs = []
        code = row.get("품목코드")
        qty = float(row.get("수량", 0) or 0)
        # 메타 조회
        meta = master_df.loc[master_df["품목코드"] == code]
        if meta.empty:
            return errs
        meta = meta.iloc[0]
        mn = meta.get("최소수량")
        mx = meta.get("최대수량")
        pack = meta.get("묶음단위")
        if pd.notna(mn) and qty > 0 and qty < float(mn):
            errs.append(f"[{row.get('품목명')}] 최소수량 {int(mn)} 이상")
        if pd.notna(mx) and qty > float(mx):
            errs.append(f"[{row.get('품목명')}] 최대수량 {int(mx)} 이하")
        if pd.notna(pack) and qty > 0 and (qty % float(pack) != 0):
            errs.append(f"[{row.get('품목명')}] {int(pack)} 단위 묶음만 허용")
        return errs

    if st.button("📦 발주 제출", type="primary", use_container_width=True):
        pick = edited[edited["수량"].fillna(0).astype(float) > 0].copy()
        if pick.empty:
            st.warning("수량이 0보다 큰 품목이 없습니다.")
            st.stop()
        # 검증
        all_errs = []
        for _, r in pick.iterrows():
            all_errs += _validate_qty(r)
        if all_errs:
            st.error("다음 항목을 확인해 주세요:\n- " + "\n- ".join(all_errs))
            st.stop()
        if not confirm:
            st.warning("체크박스로 제출 전 확인을 완료해 주세요.")
            st.stop()

        user = st.session_state["auth"]
        seq = st.session_state.get("order_seq", 1)
        order_id = make_order_id(user.get("user_id","STORE"), seq)
        st.session_state["order_seq"] = seq + 1

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for _, r in pick.iterrows():
            rows.append({
                "주문일시": now,
                "발주번호": order_id,
                "지점ID": user.get("user_id"),
                "지점명": user.get("name"),
                "품목코드": r.get("품목코드"),
                "품목명": r.get("품목명"),
                "단위": r.get("단위"),
                "수량": r.get("수량"),
                "비고": memo or "",
                "상태": "접수",
                "처리일시": "",
                "처리자": ""
            })
        ok = append_orders(rows)
        if ok:
            st.success(f"발주가 접수되었습니다. 발주번호: {order_id}")
            load_orders_df.clear()
        else:
            st.error("발주 저장에 실패했습니다.")

# =============================================================================
# 7) 관리자 화면 (주문관리, 품목/가격)
# =============================================================================
def page_admin_orders():
    st.subheader("📋 주문관리")
    df = load_orders_df()

    c1, c2, c3, c4 = st.columns([1,1,1,2])
    with c1:
        dt_from = st.date_input("시작일", value=date.today() - timedelta(days=3))
    with c2:
        dt_to = st.date_input("종료일", value=date.today())
    with c3:
        stores = ["(전체)"] + sorted(df["지점명"].dropna().unique().tolist()) if not df.empty else ["(전체)"]
        store = st.selectbox("지점", stores)
    with c4:
        status = st.multiselect("상태", ORDER_STATUSES, default=ORDER_STATUSES) if not df.empty else []

    if not df.empty:
        def _to_dt(s):
            try: return pd.to_datetime(s)
            except: return pd.NaT
        df["주문일시_dt"] = df["주문일시"].apply(_to_dt)
        mask = (df["주문일시_dt"].dt.date >= dt_from) & (df["주문일시_dt"].dt.date <= dt_to)
        if store != "(전체)": mask &= (df["지점명"] == store)
        if status: mask &= df["상태"].isin(status)
        dfv = df[mask].copy().sort_values(["주문일시","발주번호"])
    else:
        dfv = df.copy()

    st.caption(f"조회 건수: {len(dfv):,}건")
    st.dataframe(dfv, use_container_width=True, height=420)

    csv = dfv.to_csv(index=False).encode("utf-8-sig")
    st.download_button("CSV 다운로드", data=csv, file_name="orders.csv", mime="text/csv")

    st.markdown("---")
    st.markdown("**출고 처리**")
    if not dfv.empty:
        order_ids = sorted(dfv["발주번호"].unique().tolist())
        sel_ids = st.multiselect("발주번호 선택", order_ids)
        if st.button("선택 발주 출고완료 처리", type="primary"):
            if sel_ids:
                ok = update_order_status(sel_ids, new_status="출고완료", handler=st.session_state["auth"].get("name","관리자"))
                if ok:
                    st.success("출고완료 처리되었습니다.")
                    st.rerun()
                else:
                    st.error("상태 변경 실패")
            else:
                st.warning("발주번호를 선택하세요.")

def page_admin_items(master_df: pd.DataFrame):
    st.subheader("🏷️ 품목/가격")
    st.caption("※ ‘상품마스터’ 시트를 직접 수정하면 이 화면에 즉시 반영됩니다. (여기서는 조회 전용)")

    view_cols = [c for c in ["품목코드","품목명","분류","단위","단가","최소수량","최대수량","묶음단위","활성","기본리드타임","안전재고"] if c in master_df.columns]
    st.dataframe(master_df[view_cols], use_container_width=True, height=480)

# =============================================================================
# 8) 라우팅
# =============================================================================
if __name__ == "__main__":
    st.title("📦 식자재 발주 시스템")
    st.caption("무료 지향 · Google Sheets 연동 샘플")

    if not require_login():
        st.stop()

    user = st.session_state["auth"]
    role = user.get("role", "store")
    master = load_master_df()

    st.markdown("""
    <div class="small">
    ※ 본 샘플은 <span class="tag ok">Google Sheets</span> 중심으로 동작합니다. 연결이 없으면 로컬 CSV 백업만 저장됩니다.<br/>
    ※ 운영 이전에 <b>SPREADSHEET_KEY</b>와 <b>secrets.users</b>, <b>secrets.google</b>을 반드시 설정하세요.
    </div>
    """, unsafe_allow_html=True)

    if role == "admin":
        tab1, tab2 = st.tabs(["발주", "관리자"])
        with tab1:
            page_store(master)
        with tab2:
            sub1, sub2 = st.tabs(["주문관리", "품목/가격"])
            with sub1: page_admin_orders()
            with sub2: page_admin_items(master)
    else:
        # 지점은 발주 탭만
        page_store(master)
