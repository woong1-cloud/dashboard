"""
재고 대시보드 - Streamlit 버전 (Streamlit Cloud 배포용)
실행: streamlit run app.py
"""
from __future__ import annotations

import datetime as dt
import io
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from inventory_core import (
    compute_daily_change,
    get_conn,
    load_history,
    load_latest,
    normalize_excel,
    update_channel_stock,
    update_distribution_note,
    update_warehouse_stock,
    upsert_snapshot,
)

APP_TITLE = "재고 대시보드 V2"
DEFAULT_PASSWORD = "1234"
DEPLOY_MODE = os.environ.get("DEPLOY_MODE", "").strip().lower() in ("1", "true", "yes")


def get_password_from_db() -> str:
    try:
        conn = get_conn()
        conn.execute(
            "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)"
        )
        cur = conn.execute("SELECT value FROM settings WHERE key = 'password'")
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0])
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('password', ?)",
            (DEFAULT_PASSWORD,),
        )
        conn.commit()
        return DEFAULT_PASSWORD
    except Exception:
        return DEFAULT_PASSWORD


def set_password_in_db(new_password: str) -> None:
    new_password = (new_password or "").strip()
    if not new_password:
        raise ValueError("비밀번호는 빈 값일 수 없습니다.")
    conn = get_conn()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)"
    )
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('password', ?)",
        (new_password,),
    )
    conn.commit()


def init_session():
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    if "failed_csv_path" not in st.session_state:
        st.session_state.failed_csv_path = None
    if "failed_count" not in st.session_state:
        st.session_state.failed_count = 0


def login_page():
    init_session()
    st.title("🔐 로그인")
    pw = get_password_from_db()
    with st.form("login"):
        p = st.text_input("비밀번호", type="password", placeholder="비밀번호 입력")
        submitted = st.form_submit_button("로그인")
        if submitted and p and p.strip() == pw:
            st.session_state.logged_in = True
            st.rerun()
        elif submitted and (not p or p.strip() != pw):
            st.error("비밀번호가 올바르지 않습니다.")
    st.caption(f"기본 비밀번호: {DEFAULT_PASSWORD} (최초 실행 시)")


def run_upload():
    st.subheader("📤 엑셀 업로드")
    with st.form("upload_form"):
        snapshot_date = st.date_input("스냅샷 날짜", value=dt.date.today())
        sales_file = st.file_uploader(
            "상품분석판매 (필수) - CSV/Excel",
            type=["csv", "xlsx", "xls", "xlsb"],
            key="sales",
        )
        warehouse_file = st.file_uploader(
            "물류센터1 재고 (선택) - Excel",
            type=["xlsx", "xls", "xlsb"],
            key="wh1",
        )
        warehouse_file2 = st.file_uploader(
            "물류센터2 재고 (선택) - Excel",
            type=["xlsx", "xls", "xlsb"],
            key="wh2",
        )
        channel_file = st.file_uploader(
            "매장 재고 (선택) - Excel",
            type=["xlsx", "xls", "xlsb"],
            key="ch",
        )
        distribution_file = st.file_uploader(
            "분배내역 (선택) - Excel",
            type=["xlsx", "xls", "xlsb"],
            key="dist",
        )
        submitted = st.form_submit_button("📤 업로드")

    if not submitted:
        return

    if not sales_file:
        st.error("상품분석판매 파일을 선택하세요.")
        return

    date = snapshot_date
    try:
        conn = get_conn()
        # 1. 상품분석판매
        sales_ext = Path(sales_file.name or "").suffix.lower()
        if sales_ext == ".csv":
            try:
                sales_df = pd.read_csv(sales_file)
            except UnicodeDecodeError:
                sales_file.seek(0)
                sales_df = pd.read_csv(sales_file, encoding="cp949")
        else:
            sales_df = pd.read_excel(sales_file, sheet_name=0)

        result = normalize_excel(sales_df, snapshot_date=date, return_failed=True)
        if isinstance(result, tuple):
            sales_snap, failed_df = result
            if not failed_df.empty:
                st.session_state.failed_csv_path = f"failed_upload_{date.isoformat()}.csv"
                failed_df.to_csv(
                    st.session_state.failed_csv_path, index=False, encoding="utf-8-sig"
                )
                st.session_state.failed_count = len(failed_df)
        else:
            sales_snap = result

        sales_count = upsert_snapshot(conn, sales_snap)

        msg_parts = [f"상품분석판매: {sales_count}개 품목"]

        # 2. 물류센터1
        if warehouse_file and warehouse_file.name:
            wh1_df = pd.read_excel(warehouse_file, sheet_name=0)
            wh1_snap = normalize_excel(wh1_df, snapshot_date=date)
            if not wh1_snap.empty:
                sku_map = {}
                for _, row in wh1_snap.iterrows():
                    sku = str(row["sku"]).strip()
                    if sku and len(sku) == 15:
                        sku_map[sku] = int(row.get("warehouse_stock") or 0)
                if sku_map:
                    c = update_warehouse_stock(
                        conn, date.isoformat(), sku_map, warehouse_num=1
                    )
                    msg_parts.append(f"물류센터1: {c}개 SKU")

        # 3. 물류센터2
        if warehouse_file2 and warehouse_file2.name:
            wh2_df = pd.read_excel(warehouse_file2, sheet_name=0)
            wh2_snap = normalize_excel(wh2_df, snapshot_date=date)
            if not wh2_snap.empty:
                sku_map = {}
                for _, row in wh2_snap.iterrows():
                    sku = str(row["sku"]).strip()
                    if sku and len(sku) == 15:
                        sku_map[sku] = int(row.get("warehouse_stock") or 0)
                if sku_map:
                    c = update_warehouse_stock(
                        conn, date.isoformat(), sku_map, warehouse_num=2
                    )
                    msg_parts.append(f"물류센터2: {c}개 SKU")

        # 4. 매장재고
        if channel_file and channel_file.name:
            ch_df = pd.read_excel(channel_file, sheet_name=0)
            ch_snap = normalize_excel(ch_df, snapshot_date=date)
            if not ch_snap.empty:
                sku_map = {}
                for _, row in ch_snap.iterrows():
                    sku = str(row["sku"]).strip()
                    if sku and len(sku) == 15:
                        sku_map[sku] = int(row.get("channel_stock") or 0)
                if sku_map:
                    c = update_channel_stock(conn, date.isoformat(), sku_map)
                    msg_parts.append(f"매장재고: {c}개 SKU")

        # 5. 분배내역 (간단 처리)
        if distribution_file and distribution_file.name:
            dist_df = pd.read_excel(distribution_file, sheet_name=0)
            dist_df.columns = [str(c).strip() for c in dist_df.columns]
            sku_col = None
            for col in ["SKU", "상품코드", "상품", "품목코드", "sku"]:
                if col in dist_df.columns:
                    sku_col = col
                    break
            qty_col = None
            for col in ["분배량", "수량", "분배수량"]:
                if col in dist_df.columns:
                    qty_col = col
                    break
            if sku_col and qty_col:
                sku_note_map = {}
                for _, row in dist_df.iterrows():
                    sku = str(row.get(sku_col, "")).strip()[:15]
                    if not sku or sku == "nan":
                        continue
                    val = row.get(qty_col)
                    qty = int(pd.to_numeric(val, errors="coerce")) if not pd.isna(val) else 0
                    sku_note_map[sku] = str(sku_note_map.get(sku, 0) + qty) if sku in sku_note_map else str(qty)
                if sku_note_map:
                    c = update_distribution_note(conn, date.isoformat(), sku_note_map)
                    msg_parts.append(f"분배내역: {c}개 SKU")

        st.success(f"✅ {', '.join(msg_parts)} 업로드 완료 (날짜: {date})")
        if st.session_state.get("failed_count", 0) > 0:
            st.warning(
                f"⚠️ {st.session_state.failed_count}개 행이 업로드 실패했습니다. "
                "아래에서 실패 목록을 다운로드하세요."
            )
    except Exception as e:
        st.error(f"업로드 실패: {e}")
        import traceback
        traceback.print_exc()


def run_dashboard():
    st.subheader("📊 대시보드")
    conn = get_conn()
    latest_date, latest = load_latest(conn)

    if latest_date is None or latest.empty:
        st.info("업로드된 데이터가 없습니다. 엑셀을 업로드해 주세요.")
        return

    for col in (
        "sales_qty",
        "channel_stock",
        "warehouse_stock",
        "warehouse1_stock",
        "warehouse2_stock",
        "min_stock",
        "lead_time_days",
        "safety_stock",
    ):
        if col not in latest.columns:
            latest[col] = 0
    if "distribution_note" not in latest.columns:
        latest["distribution_note"] = ""

    latest["sales_qty"] = latest["sales_qty"].fillna(0).astype(int)
    latest["daily_sales_7d"] = (latest["sales_qty"] / 7.0).round(2)
    latest["channel_stock"] = latest["channel_stock"].fillna(0).astype(int)
    latest["warehouse_stock"] = latest["warehouse_stock"].fillna(0).astype(int)
    latest["total_available"] = latest["stock"] + latest["warehouse_stock"]
    latest["days_until_out"] = 999.0
    mask = latest["daily_sales_7d"] > 0
    latest.loc[mask, "days_until_out"] = (
        latest.loc[mask, "total_available"] / latest.loc[mask, "daily_sales_7d"]
    ).round(1)
    latest.loc[latest["total_available"] == 0, "days_until_out"] = 0.0

    latest["min_stock"] = latest["min_stock"].fillna(0).astype(int)
    latest["lead_time_days"] = latest["lead_time_days"].fillna(7).astype(int)
    latest["safety_stock"] = latest["safety_stock"].fillna(0).astype(int)
    target_cover_days = 14
    latest["reorder_point"] = latest["safety_stock"] + (
        latest["daily_sales_7d"] * latest["lead_time_days"]
    )
    latest["suggested_order_qty"] = (
        (latest["daily_sales_7d"] * target_cover_days) - latest["total_available"]
    ).clip(lower=0).astype(int)

    import numpy as np
    conditions = [
        (latest["stock"] == 0) & (latest["daily_sales_7d"] > 0),
        (latest["stock"] == 0),
        (latest["daily_sales_7d"] > 0) & (latest["days_until_out"] < 7),
        (latest["stock"] <= 10) & (latest["daily_sales_7d"] > 0),
        (latest["stock"] < latest["min_stock"]) & (latest["min_stock"] > 0),
        (latest["stock"] <= latest["reorder_point"]) & (latest["daily_sales_7d"] > 0),
    ]
    choices = ["긴급필업", "재고없음", "필업필요", "체크필요", "저재고", "필업검토"]
    latest["status"] = np.select(conditions, choices, default="정상")

    st.caption(f"최신 스냅샷: {latest_date}")

    total_items = int(latest["sku"].nunique())
    total_stock = int(latest["stock"].sum())
    oos = int((latest["status"] == "긴급필업").sum())
    stockout_count = int((latest["stock"] == 0).sum())
    stockout_rate = (
        round((stockout_count / total_items * 100), 1) if total_items > 0 else 0.0
    )

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("품목 수", f"{total_items:,}")
    col2.metric("현재재고 합계", f"{total_stock:,}")
    col3.metric("긴급필업", oos)
    col4.metric("결품 수", stockout_count)
    col5.metric("결품률", f"{stockout_rate}%")

    # 필터
    low_only = st.checkbox("저재고/필업만 보기", key="low_only")
    view = latest.copy()
    if low_only:
        view = view[
            view["status"].isin(
                ["긴급필업", "재고없음", "필업필요", "체크필요", "저재고", "필업검토"]
            )
        ]

    display_cols = [
        "status",
        "sku",
        "name",
        "stock",
        "channel_stock",
        "warehouse_stock",
        "daily_sales_7d",
        "days_until_out",
        "suggested_order_qty",
        "distribution_note",
    ]
    display_cols = [c for c in display_cols if c in view.columns]
    st.dataframe(
        view[display_cols].sort_values("daily_sales_7d", ascending=False),
        use_container_width=True,
        height=400,
    )

    # SKU 히스토리 차트
    sku_list = sorted(latest["sku"].astype(str).unique().tolist())
    if sku_list:
        sku_pick = st.selectbox("SKU 재고 변동 차트", options=sku_list, key="sku_pick")
        if sku_pick:
            hist = load_history(conn, sku_pick)
            if len(hist) >= 2:
                h = compute_daily_change(hist)
                h["snapshot_date"] = pd.to_datetime(h["snapshot_date"])
                fig = px.line(
                    h, x="snapshot_date", y="stock", markers=True,
                    title=f"SKU {sku_pick} 재고 변동"
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)


def run_backup():
    st.subheader("💾 백업 / 내보내기")
    conn = get_conn()
    latest_date, latest = load_latest(conn)

    if latest_date is not None and not latest.empty:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            latest.to_excel(writer, index=False, sheet_name="재고현황")
        output.seek(0)
        st.download_button(
            label="📥 엑셀 내보내기 (최신 스냅샷)",
            data=output,
            file_name=f"재고현황_{latest_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_excel",
        )
    else:
        st.info("내보낼 데이터가 없습니다.")

    db_path = Path(__file__).resolve().parent / "inventory.db"
    if db_path.exists():
        with open(db_path, "rb") as f:
            db_bytes = f.read()
        st.download_button(
            label="📥 전체 DB 백업",
            data=db_bytes,
            file_name=f"inventory_backup_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.db",
            mime="application/x-sqlite3",
            key="dl_db",
        )
    else:
        st.info("데이터베이스 파일이 없습니다.")

    if st.session_state.get("failed_csv_path") and os.path.exists(
        st.session_state["failed_csv_path"]
    ):
        with open(st.session_state["failed_csv_path"], "rb") as f:
            failed_bytes = f.read()
        st.download_button(
            label=f"📥 업로드 실패 목록 ({st.session_state.get('failed_count', 0)}건)",
            data=failed_bytes,
            file_name=f"업로드실패목록_{dt.date.today()}.csv",
            mime="text/csv",
            key="dl_failed",
        )


def run_change_password():
    st.subheader("🔑 비밀번호 변경")
    current = get_password_from_db()
    with st.form("chpw"):
        cur = st.text_input("현재 비밀번호", type="password")
        new = st.text_input("새 비밀번호", type="password")
        confirm = st.text_input("새 비밀번호 확인", type="password")
        if st.form_submit_button("변경"):
            if not cur or cur != current:
                st.error("현재 비밀번호가 올바르지 않습니다.")
            elif not new:
                st.error("새 비밀번호를 입력하세요.")
            elif new != confirm:
                st.error("새 비밀번호와 확인이 일치하지 않습니다.")
            else:
                try:
                    set_password_in_db(new)
                    st.success("비밀번호가 변경되었습니다.")
                except Exception as e:
                    st.error(str(e))


def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide", initial_sidebar_state="auto")
    init_session()

    if not st.session_state.logged_in:
        login_page()
        return

    # 사이드바: 로그아웃 + 메뉴
    with st.sidebar:
        st.title(APP_TITLE)
        if st.button("로그아웃"):
            st.session_state.logged_in = False
            st.rerun()
        page = st.radio(
            "메뉴",
            ["📊 대시보드", "📤 업로드", "💾 백업/내보내기", "🔑 비밀번호 변경"],
            label_visibility="collapsed",
        )

    if page == "📤 업로드":
        run_upload()
    elif page == "📊 대시보드":
        run_dashboard()
    elif page == "💾 백업/내보내기":
        run_backup()
    elif page == "🔑 비밀번호 변경":
        run_change_password()


if __name__ == "__main__":
    main()
