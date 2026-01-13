from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional
import xml.etree.ElementTree as ET

NAMESPACE_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"

FIELD_MAP = {
    "A": "tag_number",
    "C": "part_description",
    "E": "rejection_type",
    "G": "rejection_class",
    "I": "defect_description",
    "K": "rejected_by",
    "M": "containment_date",
    "O": "total_rejected_qty_initial",
    "Q": "mfg_date",
    "S": "mfg_shift",
    "U": "supplier",
    "W": "total_rejected_qty_final",
    "Y": "disposition",
    "AA": "authorized_by",
    "AC": "date_authorized",
    "AE": "date_sort_rework",
    "AG": "good_pcs",
    "AI": "reworked_pcs",
    "AK": "scrap_pcs",
    "AM": "complete",
    "AO": "closed_date",
    "AQ": "closed_by",
    "AS": "qad_number",
    "AU": "qad_date",
    "AW": "completed_by",
}

DATE_FIELDS = {
    "containment_date",
    "mfg_date",
    "date_authorized",
    "date_sort_rework",
    "closed_date",
    "qad_date",
}

BASE_DATE = datetime(1899, 12, 30)  # Excel serial 1


def excel_serial_to_iso(value: str) -> Optional[str]:
    """Convert an Excel serial (stored as string) to ISO date, or None."""
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        serial = float(text)
    except ValueError:
        return None
    # Guard against Excel bug where 60 is 1900-02-29
    days = int(serial)
    try:
        date_value = BASE_DATE + timedelta(days=days)
    except OverflowError:
        return None
    return date_value.date().isoformat()


@dataclass
class ExcelData:
    records: List[Dict[str, Optional[str]]]
    dropdowns: Dict[str, List[str]]


class NonconExcelLoader:
    def __init__(self, workbook_path: Path):
        self.workbook_path = Path(workbook_path)
        if not self.workbook_path.exists():
            raise FileNotFoundError(f"Workbook not found: {self.workbook_path}")
        self._zip: zipfile.ZipFile | None = None
        self._shared_strings: List[str] | None = None

    def load(self) -> ExcelData:
        with zipfile.ZipFile(self.workbook_path) as zf:
            self._zip = zf
            self._shared_strings = self._read_shared_strings(zf)
            sheet_targets = self._sheet_targets(zf)

            log_sheet_path = self._find_log_sheet(zf, sheet_targets)
            if not log_sheet_path:
                raise ValueError("Could not locate NC Log worksheet in workbook.")
            dropdown_sheet_path = self._find_dropdown_sheet(zf, sheet_targets)
            if not dropdown_sheet_path:
                raise ValueError("Could not locate dropdown worksheet in workbook.")

            records = self._read_log_records(zf, log_sheet_path)
            dropdowns = self._read_dropdowns(zf, dropdown_sheet_path)
            return ExcelData(records=records, dropdowns=dropdowns)

    # Internal helpers -------------------------------------------------

    def _sheet_targets(self, zf: zipfile.ZipFile) -> Dict[str, str]:
        workbook_xml = ET.fromstring(zf.read("xl/workbook.xml"))
        sheets = []
        for sheet in workbook_xml.findall(f"{{{NAMESPACE_MAIN}}}sheets/{{{NAMESPACE_MAIN}}}sheet"):
            name = sheet.attrib.get("name", "")
            rid = sheet.attrib.get(f"{{{REL_NS}}}id")
            if rid:
                sheets.append((name, rid))

        rels_xml = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rels = {
            rel.attrib["Id"]: rel.attrib["Target"]
            for rel in rels_xml.findall("{http://schemas.openxmlformats.org/package/2006/relationships}Relationship")
            if rel.attrib.get("Type", "").endswith("/worksheet")
        }

        sheet_targets: Dict[str, str] = {}
        for name, rid in sheets:
            target = rels.get(rid)
            if not target:
                continue
            # Targets are relative to xl/
            if not target.startswith("/"):
                sheet_targets[name] = f"xl/{target}"
            else:
                sheet_targets[name] = target.lstrip("/")
        return sheet_targets

    def _find_log_sheet(self, zf: zipfile.ZipFile, sheet_targets: Dict[str, str]) -> Optional[str]:
        for name, path in sheet_targets.items():
            if name.lower().startswith("nc log"):
                return path
        # As a fallback, inspect sheets for the "Tag Number" header
        for path in sheet_targets.values():
            if self._sheet_contains_text(zf, path, "Tag Number"):
                return path
        return None

    def _find_dropdown_sheet(self, zf: zipfile.ZipFile, sheet_targets: Dict[str, str]) -> Optional[str]:
        for name, path in sheet_targets.items():
            lower = name.lower()
            if "sheet" in lower:
                if self._sheet_contains_text(zf, path, "Rejection Type:"):
                    return path
        # fallback: search every sheet
        for path in sheet_targets.values():
            if self._sheet_contains_text(zf, path, "Rejection Type:"):
                return path
        return None

    def _sheet_contains_text(self, zf: zipfile.ZipFile, path: str, needle: str) -> bool:
        root = ET.fromstring(zf.read(path))
        for cell in root.findall(f"{{{NAMESPACE_MAIN}}}sheetData/{{{NAMESPACE_MAIN}}}row/{{{NAMESPACE_MAIN}}}c"):
            value = self._cell_value(cell)
            if value and needle.lower() in value.lower():
                return True
        return False

    def _read_shared_strings(self, zf: zipfile.ZipFile) -> List[str]:
        try:
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
        except KeyError:
            return []
        strings: List[str] = []
        for si in root.findall(f"{{{NAMESPACE_MAIN}}}si"):
            text_parts = [t.text or "" for t in si.findall(f".//{{{NAMESPACE_MAIN}}}t")]
            strings.append("".join(text_parts))
        return strings

    def _cell_value(self, cell: ET.Element) -> Optional[str]:
        v = cell.find(f"{{{NAMESPACE_MAIN}}}v")
        if v is None:
            return None
        cell_type = cell.attrib.get("t")
        raw = v.text or ""
        if cell_type == "s":
            shared = self._shared_strings[int(raw)] if self._shared_strings else raw
            return shared
        return raw

    def _read_log_records(self, zf: zipfile.ZipFile, path: str) -> List[Dict[str, Optional[str]]]:
        sheet = ET.fromstring(zf.read(path))
        records: List[Dict[str, Optional[str]]] = []
        rows = sheet.findall(f"{{{NAMESPACE_MAIN}}}sheetData/{{{NAMESPACE_MAIN}}}row")
        for row in rows:
            row_index = int(row.attrib.get("r", "0"))
            if row_index < 7:  # skip headers and intro blocks
                continue
            record: Dict[str, Optional[str]] = {}
            has_text = False
            for cell in row.findall(f"{{{NAMESPACE_MAIN}}}c"):
                match = re.match(r"([A-Z]+)", cell.attrib.get("r", ""))
                if not match:
                    continue
                column = match.group(1)
                field = FIELD_MAP.get(column)
                if not field:
                    continue
                value = self._cell_value(cell)
                if value is None:
                    continue
                value = value.strip()
                if not value:
                    continue
                has_text = True
                if field in DATE_FIELDS:
                    iso = excel_serial_to_iso(value)
                    record[field] = iso or value
                else:
                    record[field] = value
            if has_text:
                records.append(record)
        return records

    def _read_dropdowns(self, zf: zipfile.ZipFile, path: str) -> Dict[str, List[str]]:
        sheet = ET.fromstring(zf.read(path))
        dropdowns = {
            "rejection_type": [],
            "rejection_class": [],
            "disposition": [],
        }
        for cell in sheet.findall(f"{{{NAMESPACE_MAIN}}}sheetData/{{{NAMESPACE_MAIN}}}row/{{{NAMESPACE_MAIN}}}c"):
            match = re.match(r"([A-Z]+)([0-9]+)", cell.attrib.get("r", ""))
            if not match:
                continue
            column, row_no = match.groups()
            if row_no == "1":
                continue  # skip headers
            value = self._cell_value(cell)
            if not value:
                continue
            value = value.strip()
            if column == "A":
                dropdowns["rejection_type"].append(value)
            elif column == "C":
                dropdowns["rejection_class"].append(value)
            elif column == "E":
                dropdowns["disposition"].append(value)
        # Deduplicate while preserving order
        for key, values in dropdowns.items():
            seen = set()
            unique: List[str] = []
            for item in values:
                if item not in seen:
                    unique.append(item)
                    seen.add(item)
            dropdowns[key] = unique
        return dropdowns


def load_excel(workbook_path: Path | str) -> ExcelData:
    loader = NonconExcelLoader(Path(workbook_path))
    return loader.load()
