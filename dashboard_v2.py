"""
재고 대시보드 V2 - 오류 수정 및 최적화 버전
포트: 5003
"""
from __future__ import annotations

import datetime as dt
import os
from collections import defaultdict
from functools import wraps
from typing import Any, Callable, Optional

import pandas as pd
import plotly.express as px
from flask import (
    Flask,
    abort,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
    send_file,
)

from inventory_core import (
    avg_daily_usage_from_history,
    compute_daily_change,
    get_conn,
    load_history,
    load_latest,
    normalize_excel,
    reorder_suggestion,
    upsert_snapshot,
    update_channel_stock,
    update_warehouse_stock,
    update_distribution_note,
)


APP_TITLE = "재고 대시보드 V2"
DEFAULT_PASSWORD = "1234"

# 팀 배포 모드: True면 초기화 기능 비활성화, /test 비노출, 500 에러 시 상세 미표시
DEPLOY_MODE = os.environ.get("DEPLOY_MODE", "").strip().lower() in ("1", "true", "yes")


def create_app() -> Flask:
    """Flask 앱 생성 및 설정"""
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "dev-secret-key-change-me-v2")
    app.config["PROPAGATE_EXCEPTIONS"] = True
    app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50MB 제한
    return app


app = create_app()


@app.errorhandler(500)
def internal_error(e):
    """500 에러 핸들러 (배포 모드에서는 상세 미표시)"""
    if DEPLOY_MODE:
        return (
            "<h1>500 Internal Server Error</h1>"
            "<p>일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.</p>",
            500,
        )
    import traceback
    tb = traceback.format_exc()
    return (
        f"<h1>500 Internal Server Error</h1>"
        f"<pre style='background:#fee;padding:1em;border-radius:8px;'>{tb}</pre>",
        500,
    )


@app.errorhandler(404)
def not_found(e):
    """404 에러 핸들러"""
    return "<h1>404 Not Found</h1><p>요청한 페이지를 찾을 수 없습니다.</p>", 404


@app.context_processor
def inject_deploy_config():
    """템플릿에 배포 설정 전달 (초기화 버튼 노출 여부)"""
    return {"show_clear_data": not DEPLOY_MODE}


@app.route("/test")
def test():
    """서버 상태 확인 (배포 모드에서는 비노출)"""
    if DEPLOY_MODE:
        abort(404)
    return "<h1>OK</h1><p>✅ 대시보드 V2 서버 정상 작동중 (포트: 5003)</p>"


def _get_password_from_db() -> str:
    """DB에서 비밀번호 조회 (없으면 기본값으로 초기화)"""
    try:
        conn = get_conn()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        cur = conn.execute("SELECT value FROM settings WHERE key = 'password'")
        row = cur.fetchone()
        
        if row and row[0]:
            return str(row[0])
        
        # 기본 비밀번호로 초기화
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('password', ?)",
            (DEFAULT_PASSWORD,),
        )
        conn.commit()
        return DEFAULT_PASSWORD
    except Exception as e:
        print(f"[ERROR] 비밀번호 조회 실패: {e}")
        return DEFAULT_PASSWORD


def _set_password_in_db(new_password: str) -> None:
    """DB에 비밀번호 저장"""
    new_password = (new_password or "").strip()
    if not new_password:
        raise ValueError("비밀번호는 빈 값일 수 없습니다.")
    
    try:
        conn = get_conn()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('password', ?)",
            (new_password,),
        )
        conn.commit()
    except Exception as e:
        print(f"[ERROR] 비밀번호 저장 실패: {e}")
        raise


def _expected_password() -> str:
    """로그인에 사용할 현재 비밀번호 반환"""
    return _get_password_from_db()


def login_required(view: Callable[..., Any]) -> Callable[..., Any]:
    """로그인 필수 데코레이터"""
    @wraps(view)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        expected = _expected_password()
        if expected and not session.get("authed"):
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)
    return wrapper


@app.get("/login")
def login() -> str:
    """로그인 페이지"""
    if session.get("authed"):
        return redirect(url_for("dashboard"))
    return render_template("login.html", title=APP_TITLE)


@app.post("/login")
def login_post():
    """로그인 처리"""
    expected = _expected_password()
    if not expected:
        session["authed"] = True
        return redirect(url_for("dashboard"))
    
    pw = (request.form.get("password") or "").strip()
    if pw and pw == expected:
        session["authed"] = True
        return redirect(request.args.get("next") or url_for("dashboard"))
    
    flash("비밀번호가 올바르지 않습니다.", "danger")
    return redirect(url_for("login"))


@app.get("/logout")
def logout():
    """로그아웃"""
    session.clear()
    return redirect(url_for("login"))


@app.get("/backup")
@login_required
def backup_page():
    """백업 페이지"""
    return render_template("backup.html", title=APP_TITLE)


@app.get("/export/current")
@login_required
def export_current():
    """현재 대시보드 데이터를 엑셀로 내보내기"""
    from io import BytesIO
    
    try:
        conn = get_conn()
        latest_date, latest = load_latest(conn)
        
        if latest_date is None or latest.empty:
            flash("내보낼 데이터가 없습니다.", "warning")
            return redirect(url_for("dashboard"))
        
        # 파일명 생성
        filename = f"재고현황_{latest_date}.xlsx"
        
        # 엑셀 생성
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            latest.to_excel(writer, index=False, sheet_name='재고현황')
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        flash(f"엑셀 내보내기 실패: {e}", "danger")
        import traceback
        traceback.print_exc()
        return redirect(url_for("dashboard"))


@app.get("/export/database")
@login_required
def export_database():
    """전체 데이터베이스 백업"""
    from pathlib import Path
    
    try:
        db_path = Path(__file__).parent / "inventory.db"
        
        if not db_path.exists():
            flash("데이터베이스 파일이 없습니다.", "warning")
            return redirect(url_for("dashboard"))
        
        # 현재 날짜시간으로 파일명 생성
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"inventory_backup_{timestamp}.db"
        
        return send_file(
            db_path,
            mimetype='application/x-sqlite3',
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        flash(f"DB 백업 실패: {e}", "danger")
        import traceback
        traceback.print_exc()
        return redirect(url_for("dashboard"))


@app.get("/change_password")
@login_required
def change_password_get():
    """비밀번호 변경 화면"""
    return render_template("change_password.html", title=APP_TITLE)


@app.post("/change_password")
@login_required
def change_password_post():
    """비밀번호 변경 처리"""
    current_pw = (request.form.get("current_password") or "").strip()
    new_pw = (request.form.get("new_password") or "").strip()
    confirm_pw = (request.form.get("confirm_password") or "").strip()
    
    expected = _expected_password()
    if not current_pw or current_pw != expected:
        flash("현재 비밀번호가 올바르지 않습니다.", "danger")
        return redirect(url_for("change_password_get"))
    
    if not new_pw:
        flash("신규 비밀번호를 입력하세요.", "danger")
        return redirect(url_for("change_password_get"))
    
    if new_pw != confirm_pw:
        flash("신규 비밀번호와 확인용 비밀번호가 일치하지 않습니다.", "danger")
        return redirect(url_for("change_password_get"))
    
    try:
        _set_password_in_db(new_pw)
        flash("비밀번호가 성공적으로 변경되었습니다.", "success")
        return redirect(url_for("dashboard"))
    except Exception as e:
        flash(f"비밀번호 변경에 실패했습니다: {e}", "danger")
        return redirect(url_for("change_password_get"))


@app.get("/")
def root():
    """루트 경로 리다이렉트"""
    return redirect(url_for("dashboard"))


@app.get("/upload")
@login_required
def upload_get():
    """업로드 페이지"""
    return render_template(
        "upload.html", 
        title=APP_TITLE, 
        default_date=dt.date.today().isoformat()
    )


@app.post("/upload")
@login_required
def upload_post():
    """파일 업로드 처리"""
    sales_file = request.files.get("sales_file")
    warehouse_file = request.files.get("warehouse_file")
    warehouse_file2 = request.files.get("warehouse_file2")
    channel_file = request.files.get("channel_file")
    distribution_file = request.files.get("distribution_file")
    snapshot_date = (request.form.get("snapshot_date") or "").strip()
    warehouse_sheet = (request.form.get("warehouse_sheet") or "").strip()
    warehouse2_sheet = (request.form.get("warehouse2_sheet") or "").strip()
    channel_sheet = (request.form.get("channel_sheet") or "").strip()
    distribution_sheet = (request.form.get("distribution_sheet") or "").strip()
    
    # 필수 파일 검증
    if not sales_file or not sales_file.filename:
        flash("상품분석판매 파일을 선택하세요.", "danger")
        return redirect(url_for("upload_get"))
    
    # 날짜 파싱
    try:
        date = dt.date.fromisoformat(snapshot_date) if snapshot_date else dt.date.today()
    except ValueError:
        flash("날짜 형식이 올바르지 않습니다(YYYY-MM-DD).", "danger")
        return redirect(url_for("upload_get"))
    
    try:
        conn = get_conn()
        
        # 1. 상품분석판매 업로드
        sales_filename = sales_file.filename or ""
        sales_ext = os.path.splitext(sales_filename)[1].lower()
        
        if sales_ext == ".csv":
            try:
                sales_df = pd.read_csv(sales_file)
            except UnicodeDecodeError:
                sales_file.seek(0)
                sales_df = pd.read_csv(sales_file, encoding="cp949")
        elif sales_ext in [".xlsx", ".xls", ".xlsb"]:
            sales_df = pd.read_excel(sales_file, sheet_name=0)
        else:
            flash("지원하지 않는 파일 형식입니다. CSV 또는 Excel 파일을 사용하세요.", "danger")
            return redirect(url_for("upload_get"))
        
        # return_failed=True로 호출하여 실패한 행도 받기
        result = normalize_excel(sales_df, snapshot_date=date, return_failed=True)
        if isinstance(result, tuple):
            sales_snap, failed_df = result
            # 실패한 행이 있으면 CSV로 저장
            if not failed_df.empty:
                failed_csv_path = f"failed_upload_{date.isoformat()}.csv"
                failed_df.to_csv(failed_csv_path, index=False, encoding='utf-8-sig')
                session['failed_csv_path'] = failed_csv_path
                session['failed_count'] = len(failed_df)
        else:
            sales_snap = result
        
        sales_count = upsert_snapshot(conn, sales_snap)
        
        # 2. 물류센터1 재고 업로드 (선택사항)
        warehouse1_count = 0
        if warehouse_file and warehouse_file.filename:
            warehouse_filename = warehouse_file.filename or ""
            warehouse_ext = os.path.splitext(warehouse_filename)[1].lower()
            
            if warehouse_ext in [".xlsx", ".xls", ".xlsb"]:
                warehouse_df = pd.read_excel(warehouse_file, sheet_name=(warehouse_sheet or 0))
                print(f"[INFO] 물류센터1 엑셀 컬럼: {warehouse_df.columns.tolist()}")
                print(f"[INFO] 물류센터1 엑셀 행 수: {len(warehouse_df)}")
                
                warehouse_snap = normalize_excel(warehouse_df, snapshot_date=date)
                print(f"[INFO] 정규화 후 행 수: {len(warehouse_snap)}")
                
                if warehouse_snap.empty:
                    flash("⚠️ 물류센터1 파일에서 유효한 데이터를 찾을 수 없습니다.", "warning")
                else:
                    # SKU와 물류재고 매핑
                    sku_warehouse_map = {}
                    for _, row in warehouse_snap.iterrows():
                        sku = str(row["sku"]).strip()
                        warehouse = int(row.get("warehouse_stock") or 0)
                        if sku and len(sku) == 15:
                            sku_warehouse_map[sku] = warehouse
                    
                    if sku_warehouse_map:
                        warehouse1_count = update_warehouse_stock(
                            conn, date.isoformat(), sku_warehouse_map, warehouse_num=1
                        )
                        total_warehouse = sum(sku_warehouse_map.values())
                        print(f"[INFO] 물류센터1 업로드: {len(sku_warehouse_map)}개 SKU, 총 재고: {total_warehouse}")
                        print(f"[INFO] 업데이트된 SKU: {warehouse1_count}개")
                    else:
                        flash("⚠️ 물류센터1 파일에서 15자리 SKU를 찾을 수 없습니다.", "warning")
            else:
                flash("물류센터1: Excel 파일만 지원됩니다.", "warning")
        
        # 3. 물류센터2 재고 업로드 (선택사항)
        warehouse2_count = 0
        if warehouse_file2 and warehouse_file2.filename:
            warehouse2_filename = warehouse_file2.filename or ""
            warehouse2_ext = os.path.splitext(warehouse2_filename)[1].lower()
            
            if warehouse2_ext in [".xlsx", ".xls", ".xlsb"]:
                warehouse2_df = pd.read_excel(warehouse_file2, sheet_name=(warehouse2_sheet or 0))
                print(f"[INFO] 물류센터2 엑셀 컬럼: {warehouse2_df.columns.tolist()}")
                print(f"[INFO] 물류센터2 엑셀 행 수: {len(warehouse2_df)}")
                
                warehouse2_snap = normalize_excel(warehouse2_df, snapshot_date=date)
                print(f"[INFO] 정규화 후 행 수: {len(warehouse2_snap)}")
                
                if warehouse2_snap.empty:
                    flash("⚠️ 물류센터2 파일에서 유효한 데이터를 찾을 수 없습니다.", "warning")
                else:
                    # SKU와 물류재고 매핑
                    sku_warehouse2_map = {}
                    for _, row in warehouse2_snap.iterrows():
                        sku = str(row["sku"]).strip()
                        warehouse = int(row.get("warehouse_stock") or 0)
                        if sku and len(sku) == 15:
                            sku_warehouse2_map[sku] = warehouse
                    
                    if sku_warehouse2_map:
                        warehouse2_count = update_warehouse_stock(
                            conn, date.isoformat(), sku_warehouse2_map, warehouse_num=2
                        )
                        total_warehouse2 = sum(sku_warehouse2_map.values())
                        print(f"[INFO] 물류센터2 업로드: {len(sku_warehouse2_map)}개 SKU, 총 재고: {total_warehouse2}")
                        print(f"[INFO] 업데이트된 SKU: {warehouse2_count}개")
                    else:
                        flash("⚠️ 물류센터2 파일에서 15자리 SKU를 찾을 수 없습니다.", "warning")
            else:
                flash("물류센터2: Excel 파일만 지원됩니다.", "warning")
        
        # 4. 매장 재고 업로드 (선택사항)
        channel_count = 0
        if channel_file and channel_file.filename:
            channel_filename = channel_file.filename or ""
            channel_ext = os.path.splitext(channel_filename)[1].lower()
            
            if channel_ext in [".xlsx", ".xls", ".xlsb"]:
                channel_df = pd.read_excel(channel_file, sheet_name=(channel_sheet or 0))
                print(f"[INFO] 매장재고 엑셀 컬럼: {channel_df.columns.tolist()}")
                print(f"[INFO] 매장재고 엑셀 행 수: {len(channel_df)}")
                
                channel_snap = normalize_excel(channel_df, snapshot_date=date)
                print(f"[INFO] 정규화 후 행 수: {len(channel_snap)}")
                
                if channel_snap.empty:
                    flash("⚠️ 매장재고 파일에서 유효한 데이터를 찾을 수 없습니다.", "warning")
                else:
                    # SKU와 매장재고 매핑
                    sku_channel_map = {}
                    for _, row in channel_snap.iterrows():
                        sku = str(row["sku"]).strip()
                        channel_stock = int(row.get("channel_stock") or 0)
                        if sku and len(sku) == 15:
                            sku_channel_map[sku] = channel_stock
                    
                    if sku_channel_map:
                        channel_count = update_channel_stock(
                            conn, date.isoformat(), sku_channel_map
                        )
                        total_channel = sum(sku_channel_map.values())
                        print(f"[INFO] 매장재고 업로드: {len(sku_channel_map)}개 SKU, 총 재고: {total_channel}")
                        print(f"[INFO] 업데이트된 SKU: {channel_count}개")
                    else:
                        flash("⚠️ 매장재고 파일에서 15자리 SKU를 찾을 수 없습니다.", "warning")
            else:
                flash("매장재고: Excel 파일만 지원됩니다.", "warning")
        
        # 5. 분배내역 업로드 (선택사항)
        distribution_count = 0
        if distribution_file and distribution_file.filename:
            distribution_filename = distribution_file.filename or ""
            distribution_ext = os.path.splitext(distribution_filename)[1].lower()
            if distribution_ext in [".xlsx", ".xls", ".xlsb"]:
                try:
                    dist_df = pd.read_excel(distribution_file, sheet_name=(distribution_sheet or 0))
                    dist_df.columns = [str(c).strip() for c in dist_df.columns]
                    # SKU 컬럼 후보: SKU, 상품코드, 상품, 품목코드
                    sku_col = None
                    for col in ["SKU", "상품코드", "상품", "품목코드", "sku"]:
                        if col in dist_df.columns:
                            sku_col = col
                            break
                    # 분배량 컬럼 우선 (N열 등): 분배량, 수량, N열 → 수량 합계로 표시
                    qty_col = None
                    for col in ["분배량", "수량", "분배수량"]:
                        if col in dist_df.columns:
                            qty_col = col
                            break
                    if not qty_col and len(dist_df.columns) >= 14:
                        # N열 = 14번째 컬럼(인덱스 13)
                        qty_col = dist_df.columns[13]
                    # 텍스트 비고 컬럼 (분배량 없을 때 대체)
                    note_col = None
                    for col in ["분배내역", "비고", "메모", "내역", "분배요청내역", "분배요청", "비고사항"]:
                        if col in dist_df.columns:
                            note_col = col
                            break
                    use_qty = qty_col is not None
                    use_note = note_col is not None and not use_qty
                    if sku_col and (use_qty or use_note):
                        sku_note_map = {}
                        for _, row in dist_df.iterrows():
                            sku_raw = str(row.get(sku_col, "")).strip()
                            sku = sku_raw[:15] if len(sku_raw) >= 15 else sku_raw
                            if not sku or sku == "nan":
                                continue
                            if use_qty:
                                val = row.get(qty_col)
                                qty = int(pd.to_numeric(val, errors="coerce")) if not pd.isna(val) else 0
                                if sku in sku_note_map:
                                    sku_note_map[sku] = sku_note_map[sku] + qty
                                else:
                                    sku_note_map[sku] = qty
                            else:
                                note_val = row.get(note_col)
                                note = "" if pd.isna(note_val) else str(note_val).strip()
                                if sku in sku_note_map:
                                    sku_note_map[sku] = sku_note_map[sku] + " / " + note
                                else:
                                    sku_note_map[sku] = note
                        if use_qty:
                            sku_note_map = {k: str(v) for k, v in sku_note_map.items()}
                        if sku_note_map:
                            distribution_count = update_distribution_note(
                                conn, date.isoformat(), sku_note_map
                            )
                            print(f"[INFO] 분배내역 업로드: {distribution_count}개 SKU 반영 (분배량 기준)" if use_qty else f"[INFO] 분배내역 업로드: {distribution_count}개 SKU 반영")
                    else:
                        flash("⚠️ 분배내역 파일에 SKU(또는 상품코드) 컬럼과 분배량(또는 N열/수량) 컬럼이 필요합니다.", "warning")
                except Exception as ex:
                    flash(f"⚠️ 분배내역 파일 처리 중 오류: {ex}", "warning")
            else:
                flash("분배내역: Excel 파일만 지원됩니다.", "warning")
        
        # 결과 메시지
        total_warehouse_count = warehouse1_count + warehouse2_count
        msg_parts = [f"상품분석판매: {sales_count}개 품목"]
        if warehouse1_count > 0:
            msg_parts.append(f"물류센터1: {warehouse1_count}개 SKU")
        if warehouse2_count > 0:
            msg_parts.append(f"물류센터2: {warehouse2_count}개 SKU")
        if channel_count > 0:
            msg_parts.append(f"매장재고: {channel_count}개 SKU")
        if distribution_count > 0:
            msg_parts.append(f"분배내역: {distribution_count}개 SKU")
        
        success_msg = f"✅ {', '.join(msg_parts)} 업로드 완료 (날짜: {date})"
        flash(success_msg, "success")
        
        # 실패한 행이 있으면 알림 (다운로드는 상단 배너에서 가능)
        if 'failed_count' in session and session['failed_count'] > 0:
            flash(f"⚠️ {session['failed_count']}개 행이 업로드 실패했습니다. 상단 배너에서 실패 목록을 다운로드하세요.", "warning")
        
        return redirect(url_for("dashboard"))
        
    except Exception as e:
        flash(f"업로드 실패: {e}", "danger")
        import traceback
        traceback.print_exc()
        return redirect(url_for("upload_get"))


def _status_badge(status: str) -> str:
    """상태별 Bootstrap 색상 클래스 반환"""
    status_colors = {
        "긴급필업": "danger",
        "재고없음": "dark",
        "필업필요": "warning",
        "체크필요": "info",
        "저재고": "warning",
        "필업검토": "secondary",
        "정상": "success",
    }
    return status_colors.get(status, "secondary")


@app.get("/dashboard")
@login_required
def dashboard():
    """대시보드 메인 화면"""
    try:
        return _dashboard_impl()
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[ERROR] 대시보드 오류: {e}")
        print(tb)
        return (
            "<h1>500 Internal Server Error</h1>"
            "<pre style='background:#fdd;padding:1em;overflow:auto;'>"
            + tb.replace("<", "&lt;").replace(">", "&gt;")
            + "</pre>",
            500,
        )


def _dashboard_impl():
    """대시보드 로직 구현"""
    import numpy as np
    
    conn = get_conn()
    latest_date, latest = load_latest(conn)
    
    # 데이터 없으면 빈 페이지
    if latest_date is None or latest.empty:
        return render_template("empty.html", title=APP_TITLE)
    
    # 필터 파라미터
    category = (request.args.get("category") or "(전체)").strip()
    q = (request.args.get("q") or "").strip()
    low_only = (request.args.get("low_only") or "0").strip() == "1"
    warehouse_only = (request.args.get("warehouse_only") or "0").strip() == "1"
    channel_only = (request.args.get("channel_only") or "0").strip() == "1"  # 매장재고 필터
    distribution_only = (request.args.get("distribution_only") or "0").strip() == "1"  # 분배내역 있음
    warehouse_center = (request.args.get("warehouse_center") or "전체").strip()
    season_codes_selected = request.args.getlist("season_code")  # 다중 시즌 코드 필터
    urgent_category = (request.args.get("urgent_category") or "(전체)").strip()  # 긴급주의 복종 필터
    target_cover_days = int((request.args.get("target_cover_days") or "14").strip() or 14)
    sku_pick: Optional[str] = (request.args.get("sku") or "").strip() or None
    
    # === 1. 전체 데이터 처리 (상단 KPI용) ===
    all_data = latest.copy()
    
    # 누락 컬럼 기본값 설정
    for col in ("sales_qty", "channel_stock", "warehouse_stock", "warehouse1_stock", "warehouse2_stock", 
                "min_stock", "lead_time_days", "safety_stock"):
        if col not in all_data.columns:
            all_data[col] = 0
    if "distribution_note" not in all_data.columns:
        all_data["distribution_note"] = ""
    all_data["distribution_note"] = all_data["distribution_note"].fillna("").astype(str)
    
    all_data["category"] = all_data["category"].fillna("")
    
    # 시즌 코드 추출 (SKU 5,6번째 자리 = 인덱스 4:6)
    all_data["season_code"] = all_data["sku"].astype(str).str[4:6]
    
    # 복종 코드 추출 (SKU 8번째 자리 = 인덱스 7)
    all_data["category_code"] = all_data["sku"].astype(str).str[7]
    
    # 판매량 및 재고 계산
    all_data["sales_qty"] = all_data["sales_qty"].fillna(0).astype(int)
    all_data["daily_sales_7d"] = (all_data["sales_qty"] / 7.0).round(2)
    all_data["channel_stock"] = all_data["channel_stock"].fillna(0).astype(int)
    all_data["warehouse_stock"] = all_data["warehouse_stock"].fillna(0).astype(int)
    all_data["warehouse1_stock"] = all_data["warehouse1_stock"].fillna(0).astype(int)
    all_data["warehouse2_stock"] = all_data["warehouse2_stock"].fillna(0).astype(int)
    all_data["total_available"] = all_data["stock"] + all_data["warehouse_stock"]
    
    # 재고 소진 예상일
    all_data["days_until_out"] = 999.0
    mask_has_sales = all_data["daily_sales_7d"] > 0
    all_data.loc[mask_has_sales, "days_until_out"] = (
        all_data.loc[mask_has_sales, "total_available"] / all_data.loc[mask_has_sales, "daily_sales_7d"]
    ).round(1)
    all_data.loc[(all_data["total_available"] == 0), "days_until_out"] = 0.0
    
    # 발주 제안
    all_data["min_stock"] = all_data["min_stock"].fillna(0).astype(int)
    all_data["lead_time_days"] = all_data["lead_time_days"].fillna(7).astype(int)
    all_data["safety_stock"] = all_data["safety_stock"].fillna(0).astype(int)
    all_data["reorder_point"] = all_data["safety_stock"] + (all_data["daily_sales_7d"] * all_data["lead_time_days"])
    all_data["suggested_order_qty"] = (
        (all_data["daily_sales_7d"] * target_cover_days) - all_data["total_available"]
    ).clip(lower=0).astype(int)
    
    # 상태 판단
    conditions = [
        (all_data["stock"] == 0) & (all_data["daily_sales_7d"] > 0),
        (all_data["stock"] == 0),
        (all_data["daily_sales_7d"] > 0) & (all_data["days_until_out"] < 7),
        (all_data["stock"] <= 10) & (all_data["daily_sales_7d"] > 0),
        (all_data["stock"] < all_data["min_stock"]) & (all_data["min_stock"] > 0),
        (all_data["stock"] <= all_data["reorder_point"]) & (all_data["daily_sales_7d"] > 0),
    ]
    choices = ["긴급필업", "재고없음", "필업필요", "체크필요", "저재고", "필업검토"]
    all_data["status"] = np.select(conditions, choices, default="정상")
    all_data["product_code"] = all_data["sku"].astype(str).str[:10]
    
    # 전체 KPI 계산 (상단 고정용)
    total_items_all = int(all_data["sku"].nunique())
    total_stock_all = int(all_data["stock"].sum())
    oos_all = int((all_data["status"] == "긴급필업").sum())
    low_all = int((all_data["status"] == "체크필요").sum())
    has_channel_all = int((all_data["channel_stock"] > 0).sum())
    total_channel_stock_all = int(all_data["channel_stock"].sum())
    has_warehouse_all = int((all_data["warehouse_stock"] > 0).sum())
    total_warehouse_stock_all = int(all_data["warehouse_stock"].sum())
    stockout_count_all = int((all_data["stock"] == 0).sum())
    stockout_rate_all = round((stockout_count_all / total_items_all * 100), 1) if total_items_all > 0 else 0.0
    
    # 분배내역 KPI (품목수: 분배내역 있는 행 수, 전체수량: 분배내역 값 중 숫자 합계)
    dist_note_filled = all_data["distribution_note"].fillna("").astype(str).str.strip() != ""
    distribution_items_all = int(dist_note_filled.sum())
    distribution_total_qty_all = 0
    for v in all_data.loc[dist_note_filled, "distribution_note"]:
        n = pd.to_numeric(str(v).strip(), errors="coerce")
        if pd.notna(n):
            distribution_total_qty_all += int(n)
    
    # 복종별 결품률 계산
    category_stockout_stats = []
    for cat_code in sorted(all_data["category_code"].dropna().unique()):
        cat_data = all_data[all_data["category_code"] == cat_code]
        cat_total = len(cat_data)
        cat_stockout = int((cat_data["stock"] == 0).sum())
        cat_rate = round((cat_stockout / cat_total * 100), 1) if cat_total > 0 else 0.0
        category_stockout_stats.append({
            "code": cat_code,
            "total": cat_total,
            "stockout": cat_stockout,
            "rate": cat_rate
        })
    
    # 시즌별 결품률 계산 (2자리 코드 F1, G1 등 + 첫글자 그룹)
    season_stockout_stats = []
    for sc in sorted(all_data["season_code"].dropna().unique()):
        sc_str = str(sc).strip()
        if not sc_str or len(sc_str) < 2:
            continue
        sc_data = all_data[all_data["season_code"] == sc]
        sc_total = len(sc_data)
        sc_stockout = int((sc_data["stock"] == 0).sum())
        sc_rate = round((sc_stockout / sc_total * 100), 1) if sc_total > 0 else 0.0
        season_stockout_stats.append({
            "code": sc_str,
            "total": sc_total,
            "stockout": sc_stockout,
            "rate": sc_rate,
            "letter": sc_str[0].upper(),
        })
    
    season_groups = defaultdict(list)
    for s in season_stockout_stats:
        season_groups[s["letter"]].append(s)
    
    season_group_stats = []
    for letter in sorted(season_groups.keys()):
        seasons = season_groups[letter]
        group_total = sum(se["total"] for se in seasons)
        group_stockout = sum(se["stockout"] for se in seasons)
        group_rate = round((group_stockout / group_total * 100), 1) if group_total > 0 else 0.0
        season_group_stats.append({
            "letter": letter,
            "total": group_total,
            "stockout": group_stockout,
            "rate": group_rate,
            "seasons": sorted(seasons, key=lambda x: x["code"]),
        })
    
    # 전체 데이터 기준 긴급주의 써머리 (상위 30개, 복종별 필터 적용)
    high_risk_all = all_data[
        (all_data["daily_sales_7d"] > 0) & 
        ((all_data["status"].isin(["긴급필업", "필업필요", "체크필요"])) | (all_data["days_until_out"] < 14))
    ].copy()
    
    # 긴급주의 복종 필터링
    if urgent_category != "(전체)":
        high_risk_all = high_risk_all[high_risk_all["category_code"] == urgent_category]
    
    high_risk_summary = (
        high_risk_all.sort_values("daily_sales_7d", ascending=False).head(30).to_dict(orient="records")
        if not high_risk_all.empty
        else []
    )
    
    # 긴급주의용 복종 코드 목록 (전체 데이터 기준)
    urgent_categories = ["(전체)"] + sorted(all_data["category_code"].dropna().unique().tolist())
    
    # === 2. 필터링된 데이터 처리 ===
    view = all_data.copy()
    
    # 상태 카테고리 목록
    status_list = ["(전체)", "긴급필업", "재고없음", "필업필요", "체크필요", "저재고", "필업검토", "정상"]
    categories = status_list
    
    # 시즌 코드 목록 생성 (정렬된 유니크 값)
    season_codes = ["(전체)"] + sorted(all_data["season_code"].dropna().unique().tolist())
    
    # 검색 필터링 (하이브리드: 쉼표=OR, 공백=AND)
    # 예: "SPPP G11" → SPPP AND G11
    #     "SPPP, G23" → SPPP OR G23
    #     "SPPP G11, G23 U0" → (SPPP AND G11) OR (G23 AND U0)
    if q:
        # 쉼표로 OR 그룹 분리 (pd는 모듈 상단에서 import됨)
        or_groups = [g.strip() for g in q.split(',') if g.strip()]
        final_mask = pd.Series([False] * len(view), index=view.index)
        
        for group in or_groups:
            # 각 그룹 내에서 공백으로 AND 조건 분리
            and_terms = group.lower().split()
            group_mask = pd.Series([True] * len(view), index=view.index)
            
            for term in and_terms:
                term_mask = (
                    view["sku"].astype(str).str.lower().str.contains(term, na=False) |
                    view["name"].fillna("").astype(str).str.lower().str.contains(term, na=False)
                )
                group_mask = group_mask & term_mask
            
            final_mask = final_mask | group_mask
        
        view = view[final_mask]
    
    # 상태 필터링
    if category != "(전체)":
        view = view[view["status"] == category]
    
    # 시즌 코드 필터링 (다중 선택)
    if season_codes_selected and len(season_codes_selected) > 0:
        view = view[view["season_code"].isin(season_codes_selected)]
    
    if low_only:
        view = view[view["status"].isin(["긴급필업", "재고없음", "필업필요", "체크필요", "저재고", "필업검토"])]
    
    if warehouse_only:
        view = view[view["warehouse_stock"] > 0]
    
    if channel_only:
        view = view[view["channel_stock"] > 0]
    
    if distribution_only:
        view = view[view["distribution_note"].fillna("").astype(str).str.strip() != ""]
    
    # 물류센터별 필터링
    if warehouse_center == "센터1":
        view = view[view["warehouse1_stock"] > 0]
    elif warehouse_center == "센터2":
        view = view[view["warehouse2_stock"] > 0]
    
    # 필터링된 데이터에 avg_daily_usage_est 추가 (테이블 표시용)
    view["avg_daily_usage_est"] = 0.0
    
    # 필터링된 KPI 계산
    filtered_items = int(view["sku"].nunique())
    filtered_stockout_count = int((view["stock"] == 0).sum())
    filtered_stockout_rate = round((filtered_stockout_count / filtered_items * 100), 1) if filtered_items > 0 else 0.0
    # 물류가용재고가 있는 품목 수 (warehouse_stock > 0) 및 비율
    filtered_warehouse_available_count = int((view["warehouse_stock"] > 0).sum())
    filtered_warehouse_available_pct = round((filtered_warehouse_available_count / filtered_items * 100), 1) if filtered_items > 0 else 0.0
    
    # 전체 테이블: 판매량 높은 순으로 정렬 (필업제안 ↔ 분배가능 사이에 분배내역)
    table_columns = [
        "status", "product_code", "sku", "name", "category", "stock", "channel_stock",
        "warehouse1_stock", "warehouse2_stock", "warehouse_stock",
        "daily_sales_7d", "days_until_out", "suggested_order_qty", "distribution_note",
        "min_stock", "reorder_point", "avg_daily_usage_est",
        "lead_time_days", "safety_stock",
    ]
    
    table = (
        view[table_columns]
        .sort_values("daily_sales_7d", ascending=False)
        .to_dict(orient="records")
    )
    
    # SKU 히스토리 차트
    sku_list = sorted(latest["sku"].astype(str).unique().tolist())
    sku_pick = sku_pick or (sku_list[0] if sku_list else None)
    chart_sku_line_html = None
    chart_sku_delta_html = None
    
    if sku_pick:
        hist = load_history(conn, sku_pick)
        if len(hist) >= 2:
            h = compute_daily_change(hist)
            h["snapshot_date"] = pd.to_datetime(h["snapshot_date"])
            
            fig_line = px.line(h, x="snapshot_date", y="stock", markers=True, title=f"SKU {sku_pick} 재고 변동")
            fig_line.update_layout(height=300)
            
            fig_delta = px.bar(h.dropna(subset=["delta"]), x="snapshot_date", y="delta", title="일별 재고 증감")
            fig_delta.update_layout(height=250)
            
            chart_sku_line_html = fig_line.to_html(full_html=False, include_plotlyjs=False)
            chart_sku_delta_html = fig_delta.to_html(full_html=False, include_plotlyjs=False)
    
    # 전체 KPI (상단 고정용)
    kpi_all = {
        "total_items": total_items_all,
        "total_stock": total_stock_all,
        "oos": oos_all,
        "low": low_all,
        "has_channel": has_channel_all,
        "total_channel_stock": total_channel_stock_all,
        "has_warehouse": has_warehouse_all,
        "total_warehouse_stock": total_warehouse_stock_all,
        "stockout_count": stockout_count_all,
        "stockout_rate": stockout_rate_all,
        "distribution_items": distribution_items_all,
        "distribution_total_qty": distribution_total_qty_all,
    }
    
    # 필터링된 KPI (필터 영역 하단용)
    kpi_filtered = {
        "total_items": filtered_items,
        "stockout_count": filtered_stockout_count,
        "stockout_rate": filtered_stockout_rate,
        "warehouse_available_count": filtered_warehouse_available_count,
        "warehouse_available_pct": filtered_warehouse_available_pct,
    }
    
    return render_template(
        "dashboard.html",
        title=APP_TITLE,
        latest_date=latest_date,
        kpi=kpi_all,  # 전체 KPI (상단)
        kpi_filtered=kpi_filtered,  # 필터링된 KPI (필터 하단)
        category_stockout_stats=category_stockout_stats,  # 복종별 결품률
        season_group_stats=season_group_stats,  # 시즌별 결품률 (첫글자 그룹 + 2자리 시즌)
        categories=categories,
        season_codes=season_codes,  # 시즌 코드 목록
        urgent_categories=urgent_categories,  # 긴급주의 복종 코드 목록
        selected={
            "category": category,
            "q": q,
            "low_only": low_only,
            "warehouse_only": warehouse_only,
            "channel_only": channel_only,
            "distribution_only": distribution_only,
            "warehouse_center": warehouse_center,
            "season_codes": season_codes_selected,  # 다중 시즌 코드 필터
            "urgent_category": urgent_category,  # 긴급주의 복종 필터
            "target_cover_days": target_cover_days,
            "sku": sku_pick,
        },
        high_risk_summary=high_risk_summary,
        table=table,
        status_badge=_status_badge,
        sku_list=sku_list,
        chart_sku_line_html=chart_sku_line_html,
        chart_sku_delta_html=chart_sku_delta_html,
    )


@app.route("/download_failed")
@login_required
def download_failed():
    """업로드 실패 목록 다운로드"""
    if 'failed_csv_path' not in session:
        flash("다운로드할 실패 목록이 없습니다.", "warning")
        return redirect(url_for("dashboard"))
    
    csv_path = session['failed_csv_path']
    if not os.path.exists(csv_path):
        flash("실패 목록 파일을 찾을 수 없습니다.", "danger")
        return redirect(url_for("dashboard"))
    
    return send_file(
        csv_path,
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'업로드실패목록_{dt.date.today()}.csv'
    )


@app.route("/clear_data", methods=["GET", "POST"])
@login_required
def clear_data():
    """데이터 초기화 (배포 모드에서는 비활성화)"""
    if DEPLOY_MODE:
        abort(404)
    if request.method == "GET":
        # 확인 페이지 표시
        conn = get_conn()
        total_count = pd.read_sql_query("SELECT COUNT(*) as cnt FROM snapshots", conn).iloc[0]['cnt']
        dates_count = pd.read_sql_query("SELECT COUNT(DISTINCT snapshot_date) as cnt FROM snapshots", conn).iloc[0]['cnt']
        return render_template(
            "clear_data.html", 
            title="데이터 초기화",
            total_count=total_count,
            dates_count=dates_count
        )
    
    # POST 요청: 실제 삭제
    confirm = request.form.get("confirm")
    if confirm == "DELETE":
        try:
            conn = get_conn()
            conn.execute("DELETE FROM snapshots")
            conn.commit()
            flash("✅ 모든 데이터가 삭제되었습니다.", "success")
            return redirect(url_for("dashboard"))
        except Exception as e:
            flash(f"❌ 데이터 삭제 실패: {e}", "danger")
            return redirect(url_for("clear_data"))
    else:
        flash("⚠️ 확인 문구가 일치하지 않습니다.", "warning")
        return redirect(url_for("clear_data"))


if __name__ == "__main__":
    import sys
    # Streamlit Cloud 등에서 streamlit run으로 실행될 때는 Flask 서버를 띄우지 않음
    if "streamlit" in sys.modules:
        # Streamlit 전용 앱은 app.py 사용: streamlit run app.py
        pass
    else:
        print("=" * 70)
        print("재고 대시보드 V2 서버 시작!")
        print("=" * 70)
        print("접속 주소: http://127.0.0.1:5003")
        print("기본 비밀번호: 1234")
        if DEPLOY_MODE:
            print("모드: 배포 (초기화 비노출, /test 비노출)")
        print("=" * 70)
        print("")
        app.run(host="127.0.0.1", port=5003, debug=not DEPLOY_MODE)
