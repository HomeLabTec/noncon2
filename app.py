from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

from flask import (
    Flask,
    abort,
    flash,
    g,
    redirect,
    render_template,
    request,
    url_for,
)

import database
from excel_loader import load_excel

BASE_DIR = Path(__file__).resolve().parent
EXCEL_PATH = BASE_DIR / "Nonconforming Log 2025.xlsx"

app = Flask(__name__)
app.config.update(
    SECRET_KEY="replace-this-with-a-random-secret",
    DATABASE=str(database.DB_PATH),
    EXCEL_PATH=str(EXCEL_PATH),
)

# -----------------------------------------------------------------------------
# Database lifecycle helpers
# -----------------------------------------------------------------------------


def get_db():
    if "db" not in g:
        g.db = database.get_connection(Path(app.config["DATABASE"]))
    return g.db


@app.teardown_appcontext
def close_db(exc: Optional[BaseException]) -> None:  # pragma: no cover - flask hook
    db = g.pop("db", None)
    if db is not None:
        db.close()


# -----------------------------------------------------------------------------
# Initial data bootstrap
# -----------------------------------------------------------------------------


def ensure_bootstrap_data() -> None:
    db = get_db()
    database.init_db(db)
    existing_count = db.execute("SELECT COUNT(*) FROM tags").fetchone()[0]
    dropdowns = database.fetch_dropdowns(db)
    needs_seed = existing_count == 0
    needs_dropdowns = not dropdowns
    if not (needs_seed or needs_dropdowns):
        return

    excel_file = Path(app.config["EXCEL_PATH"])
    if not excel_file.exists():
        raise FileNotFoundError(
            "Excel file not found. Place 'Nonconforming Log 2025.xlsx' in the project root."
        )

    excel_data = load_excel(excel_file)
    if needs_seed:
        database.seed_from_excel(db, excel_data.records)
    if needs_dropdowns:
        database.upsert_dropdowns(db, excel_data.dropdowns)


@app.before_request
def before_request() -> None:  # pragma: no cover - flask hook
    ensure_bootstrap_data()
    g.dropdown_options = database.fetch_dropdowns(get_db())


# -----------------------------------------------------------------------------
# Shared form metadata
# -----------------------------------------------------------------------------

FORM_SECTIONS = [
    {
        "title": "Quality Auditor",
        "fields": [
            {"name": "tag_number", "label": "Tag Number", "type": "text", "readonly": True},
            {"name": "part_description", "label": "Part Description", "type": "text"},
            {
                "name": "rejection_type",
                "label": "Rejection Type",
                "type": "select",
                "options_key": "rejection_type",
            },
            {
                "name": "rejection_class",
                "label": "Rejection Class",
                "type": "select",
                "options_key": "rejection_class",
            },
            {
                "name": "defect_description",
                "label": "Defect / Issue Description",
                "type": "textarea",
            },
            {"name": "rejected_by", "label": "Rejected By", "type": "text"},
            {"name": "containment_date", "label": "Containment Date", "type": "date"},
            {
                "name": "total_rejected_qty_initial",
                "label": "Total Rejected Qty",
                "type": "text",
            },
            {"name": "mfg_date", "label": "Manufacturing Date", "type": "date"},
            {"name": "mfg_shift", "label": "Manufacturing Shift", "type": "text"},
            {"name": "supplier", "label": "Supplier", "type": "text"},
            {
                "name": "total_rejected_qty_final",
                "label": "Total Quantity Rejected",
                "type": "text",
            },
        ],
    },
    {
        "title": "QE / ENG Responsible",
        "fields": [
            {
                "name": "disposition",
                "label": "Disposition",
                "type": "select",
                "options_key": "disposition",
            },
            {"name": "authorized_by", "label": "Authorized By", "type": "text"},
            {"name": "date_authorized", "label": "Date Authorized", "type": "date"},
        ],
    },
    {
        "title": "Production Supervisor Responsible",
        "fields": [
            {"name": "date_sort_rework", "label": "Date of Sort / Rework", "type": "date"},
            {"name": "good_pcs", "label": "Good Pieces", "type": "text"},
            {"name": "reworked_pcs", "label": "Reworked Pieces", "type": "text"},
            {"name": "scrap_pcs", "label": "Scrap Pieces", "type": "text"},
            {"name": "closed_date", "label": "Closed Date", "type": "date"},
            {"name": "closed_by", "label": "Closed By", "type": "text"},
        ],
    },
    {
        "title": "Materials Responsible",
        "fields": [
            {"name": "qad_number", "label": "QAD", "type": "text"},
            {"name": "qad_date", "label": "QAD Date", "type": "date"},
            {"name": "completed_by", "label": "Completed By", "type": "text"},
        ],
    },
]

FIELD_CONFIG = [field for section in FORM_SECTIONS for field in section["fields"]]

SELECT_FIELDS = {field["name"] for field in FIELD_CONFIG if field.get("type") == "select"}
DATE_FIELDS = {field["name"] for field in FIELD_CONFIG if field.get("type") == "date"}


# -----------------------------------------------------------------------------
# Utility helpers
# -----------------------------------------------------------------------------


def normalize_form_data(form: Dict[str, str]) -> Dict[str, Optional[str]]:
    data: Dict[str, Optional[str]] = {}
    for field in FIELD_CONFIG:
        name = field["name"]
        value = form.get(name)
        if name in SELECT_FIELDS and value == "__other__":
            custom_value = form.get(f"{name}_other", "").strip()
            value = custom_value or None
        if value is not None:
            value = value.strip() or None
        data[name] = value
    # Dates: ensure empty string becomes None
    for name in DATE_FIELDS:
        if data.get(name) in {"", None}:
            data[name] = None
    return data


def prepare_form_context(record: Dict[str, Optional[str]]) -> Dict[str, Dict[str, Optional[str]]]:
    dropdowns = g.dropdown_options
    select_values = {}
    other_values = {}
    for field in SELECT_FIELDS:
        current = (record.get(field) or "").strip()
        options = dropdowns.get(field, [])
        if current and current not in options:
            select_values[field] = "__other__"
            other_values[field] = current
        else:
            select_values[field] = current
    return {"select_values": select_values, "other_values": other_values}


def coerce_date_param(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------


@app.route("/")
def dashboard():
    db = get_db()
    stats = database.dashboard_stats(db)
    recent = db.execute(
        "SELECT * FROM tags ORDER BY (updated_at IS NULL), updated_at DESC LIMIT 8"
    ).fetchall()
    today = date.today()
    default_start = today - timedelta(days=30)
    start = coerce_date_param(request.args.get("start_date")) or default_start
    end = coerce_date_param(request.args.get("end_date")) or today
    if start > end:
        start, end = end, start
    start_str, end_str = start.isoformat(), end.isoformat()
    report_tab = request.args.get("report_tab", "open")
    if report_tab not in {"open", "closed"}:
        report_tab = "open"
    if report_tab == "closed":
        report_rows = database.report_rows_closed(db, start_str, end_str)
    else:
        report_rows = database.report_rows_open(db, start_str, end_str)
    report_summary = database.report_summary(report_rows)
    return render_template(
        "dashboard.html",
        stats=stats,
        recent=recent,
        report_rows=report_rows,
        report_summary=report_summary,
        report_range={"start": start_str, "end": end_str},
        report_tab=report_tab,
    )


@app.route("/tags")
def list_all_tags():
    tags = database.list_tags(get_db())
    return render_template("tags_list.html", title="All Tags", tags=tags, view="all")


@app.route("/tags/open")
def list_open_tags():
    tags = database.list_tags(get_db(), is_closed=False)
    return render_template("tags_list.html", title="Open Tags", tags=tags, view="open")


@app.route("/tags/closed")
def list_closed_tags():
    tags = database.list_tags(get_db(), is_closed=True)
    return render_template("tags_list.html", title="Closed Tags", tags=tags, view="closed")


@app.route("/tags/new", methods=["GET", "POST"])
def create_tag():
    db = get_db()
    dropdowns = g.dropdown_options
    if request.method == "POST":
        data = normalize_form_data(request.form)
        if not data.get("part_description"):
            flash("Part description is required.", "error")
        else:
            if not data.get("tag_number"):
                data["tag_number"] = database.generate_tag_number(db)
            database.insert_tag(db, data)
            flash("Tag created successfully.", "success")
            return redirect(url_for("list_open_tags"))
    else:
        suggested_tag = database.generate_tag_number(db)
        data = {field["name"]: None for field in FIELD_CONFIG}
        data["tag_number"] = suggested_tag

    form_helpers = prepare_form_context(data)
    return render_template(
        "tag_form.html",
        form_data=data,
        field_config=FIELD_CONFIG,
        form_sections=FORM_SECTIONS,
        dropdowns=dropdowns,
        **form_helpers,
        mode="create",
    )


@app.route("/tags/<int:tag_id>/edit", methods=["GET", "POST"])
def edit_tag(tag_id: int):
    db = get_db()
    dropdowns = g.dropdown_options
    row = database.get_tag(db, tag_id)
    if row is None:
        abort(404)

    data = dict(row)
    if request.method == "POST":
        updated_data = normalize_form_data(request.form)
        updated_data["tag_number"] = data.get("tag_number")
        database.update_tag(db, tag_id, updated_data)
        flash("Tag updated.", "success")
        return redirect(url_for("edit_tag", tag_id=tag_id))

    form_helpers = prepare_form_context(data)
    return render_template(
        "tag_form.html",
        form_data=data,
        field_config=FIELD_CONFIG,
        form_sections=FORM_SECTIONS,
        dropdowns=dropdowns,
        **form_helpers,
        mode="edit",
        tag_id=tag_id,
    )


@app.route("/tags/<int:tag_id>/delete", methods=["POST"])
def delete_tag(tag_id: int):
    db = get_db()
    row = database.get_tag(db, tag_id)
    if row is None:
        abort(404)

    database.delete_tag(db, tag_id)
    flash("Tag deleted.", "success")

    if row["is_closed"]:
        return redirect(url_for("list_closed_tags"))
    return redirect(url_for("list_open_tags"))


@app.route("/tags/<int:tag_id>")
def view_tag(tag_id: int):
    row = database.get_tag(get_db(), tag_id)
    if row is None:
        abort(404)
    return render_template(
        "tag_detail.html",
        tag=row,
        field_config=FIELD_CONFIG,
        form_sections=FORM_SECTIONS,
    )




@app.context_processor
def inject_globals():
    from datetime import datetime
    return {'current_year': datetime.utcnow().year}

# -----------------------------------------------------------------------------
# CLI utility
# -----------------------------------------------------------------------------


@app.cli.command("reseed")
def reseed_command():  # pragma: no cover - CLI helper
    """Re-import data from the Excel workbook."""
    db = get_db()
    excel_file = Path(app.config["EXCEL_PATH"])
    data = load_excel(excel_file)
    db.execute("DELETE FROM tags")
    db.execute("DELETE FROM dropdown_options")
    database.seed_from_excel(db, data.records)
    database.upsert_dropdowns(db, data.dropdowns)
    print("Database reseeded from Excel.")


if __name__ == "__main__":  # pragma: no cover
    app.run(debug=True, host="0.0.0.0", port=5000)
