import flet as ft
import pandas as pd
import numpy as np
import os
from datetime import datetime
from pathlib import Path
from difflib import SequenceMatcher
import json
import subprocess
import winreg as reg

# Get today's date in normalized form (no time component)
TODAY = pd.Timestamp.today().normalize()

# Load aliases from the JSON file (for auto-mapping purposes)
def load_aliases(file_path: str):
    with open(file_path, 'r') as f:
        return json.load(f)

# Load the aliases from 'aliases.json' file
ALIASES = load_aliases('aliases.json')  # Ensure 'aliases.json' is in the same directory

# Define the canonical tables supported in the wizard (order matters)
TABLE_STEPS = ["PurchaseOrder", "GRN", "Invoices", "Item", "Suppliers", "PurchaseOrderLine"]

# Define the required columns for each table (minimum necessary columns for processing)
REQUIRED_COLS = {
    "PurchaseOrder": ["PO_ID", "Supplier_ID", "PO_Date", "Handled_By", "TotalAmount", "Status", "ExpectedDeliveryDate"],
    "GRN": ["GRN_No", "PO_ID", "Qty_Received", "Date_Arrived", "Status_Arrived", "Quantity_Defect"],
    "Invoices": ["InvoiceID", "Supp_ID", "PO_ID", "Amount", "Payment_Status"],
    "Item": ["ItemID", "SuppID", "CategoryName"],
    "Suppliers": ["Supp_ID", "SuppName", "Country"],
    "PurchaseOrderLine": ["POL_ID", "PO_ID", "Item_ID", "Supp_ID"]
}

# ---------------------------
# AUTO-MAP CONFIG
# ---------------------------

def norm(s: str) -> str:
    """Normalize for matching: lowercase, remove spaces, underscores, hyphens, slashes."""
    if s is None:
        return ""
    s = str(s).strip().lower()
    for ch in [" ", "_", "-", "/", "\\", ".", ":", ";", "(", ")", "[", "]"]:
        s = s.replace(ch, "")
    return s

def similarity(a: str, b: str) -> float:
    """Return the similarity score between two strings using SequenceMatcher."""
    return SequenceMatcher(None, norm(a), norm(b)).ratio()

def best_match_for_canonical(canon_col: str, actual_cols: list[str]) -> tuple[str | None, float]:
    """Return (best_actual_col, score). Score in [0, 1]."""
    if not actual_cols:
        return None, 0.0

    canon_n = norm(canon_col)

    # 1) Exact normalized match with canonical column
    for ac in actual_cols:
        if norm(ac) == canon_n:
            return ac, 1.0

    # 2) Exact normalized match with aliases
    for alias in ALIASES.get(canon_col, []):
        alias_n = norm(alias)
        for ac in actual_cols:
            if norm(ac) == alias_n:
                return ac, 0.98

    # 3) Fuzzy best match across canonical and aliases
    candidates = [canon_col] + ALIASES.get(canon_col, [])
    best_ac, best_sc = None, 0.0
    for ac in actual_cols:
        sc = max(similarity(ac, cand) for cand in candidates)
        if sc > best_sc:
            best_sc = sc
            best_ac = ac

    if best_sc >= 0.70:
        return best_ac, float(best_sc)
    return None, float(best_sc)

def auto_map_required(table_name: str, df: pd.DataFrame) -> tuple[dict, dict]:
    """Auto-map required columns only."""
    mapping = {}
    scores = {}
    cols = list(df.columns)
    for canon in REQUIRED_COLS.get(table_name, []):
        match, sc = best_match_for_canonical(canon, cols)
        if match:
            mapping[canon] = match
        scores[canon] = sc
    return mapping, scores

# ---------------------------
# HELPERS
# ---------------------------

def import_excel_sheets(file_path: str):
    """Load all sheets from the given Excel file into a dictionary."""
    excel_data = pd.ExcelFile(file_path)
    return {name: excel_data.parse(name) for name in excel_data.sheet_names}

def validate_columns(df: pd.DataFrame, required_cols: list, table_name: str):
    """Ensure the required columns are present in the dataframe."""
    if df is None:
        raise KeyError(f"{table_name} is missing (no sheet selected).")
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"{table_name} missing required columns: {missing}")

def safe_to_datetime(series):
    """Convert a series to datetime, coerce errors to NaT (Not a Time)."""
    return pd.to_datetime(series, errors="coerce")

def filter_latest_grn(grn_df: pd.DataFrame):
    """Filter GRN to keep only the latest records based on PO_ID and Date_Arrived."""
    df = grn_df.copy()
    df["Date_Arrived"] = safe_to_datetime(df["Date_Arrived"])
    df = df.sort_values(by=["PO_ID", "Date_Arrived"], ascending=[True, False])
    return df.drop_duplicates(subset="PO_ID", keep="first")

def defect_rate(grn_df: pd.DataFrame):
    """Calculate the defect rate for each GRN."""
    df = grn_df.copy()
    if "Qty_Received" in df.columns and "Quantity_Defect" in df.columns:
        df["Defect_Rate"] = np.where(
            df["Qty_Received"] != 0,
            df["Quantity_Defect"] / df["Qty_Received"],
            np.nan
        )
    else:
        df["Defect_Rate"] = np.nan
    return df

def join_po_grn(po_df: pd.DataFrame, grn_df: pd.DataFrame):
    """Join the PurchaseOrder and GRN dataframes on PO_ID."""
    validate_columns(po_df, ["PO_ID"], "PurchaseOrder")
    validate_columns(grn_df, ["PO_ID"], "GRN")
    return po_df.merge(grn_df, on="PO_ID", how="left", suffixes=("_PO", "_GRN"))

def compute_lead_time(df: pd.DataFrame):
    """Calculate the lead time for each order, comparing expected vs actual arrival dates."""
    df = df.copy()
    df["Date_Arrived"] = safe_to_datetime(df.get("Date_Arrived"))
    df["ExpectedDeliveryDate"] = safe_to_datetime(df.get("ExpectedDeliveryDate"))

    lead = pd.Series(np.nan, index=df.index, dtype="float")

    approved = df.get("Status").eq("Approved") if "Status" in df.columns else pd.Series(False, index=df.index)
    has_arrived = approved & df["Date_Arrived"].notna()

    lead.loc[has_arrived & (df["Date_Arrived"] <= df["ExpectedDeliveryDate"])] = 0
    lead.loc[has_arrived & (df["Date_Arrived"] > df["ExpectedDeliveryDate"])] = (
        (df["Date_Arrived"] - df["ExpectedDeliveryDate"]).dt.days
    )

    not_arrived = approved & df["Date_Arrived"].isna()
    lead.loc[not_arrived & (df["ExpectedDeliveryDate"] < TODAY)] = (
        (TODAY - df["ExpectedDeliveryDate"]).dt.days
    )

    df["Lead_Time"] = lead
    return df

def on_time_late(df: pd.DataFrame):
    """Determine if an order was delivered on time or late."""
    df = df.copy()
    status = pd.Series(np.nan, index=df.index, dtype="object")

    approved = df.get("Status").eq("Approved") if "Status" in df.columns else pd.Series(False, index=df.index)
    has_arrived = approved & df["Date_Arrived"].notna()

    status.loc[has_arrived & (df["Date_Arrived"] <= df["ExpectedDeliveryDate"])] = "On-Time"
    status.loc[has_arrived & (df["Date_Arrived"] > df["ExpectedDeliveryDate"])] = "Late"

    df["Delivery_Status"] = status
    return df

def add_flags(df: pd.DataFrame):
    """Add flags for on-time and late deliveries."""
    df = df.copy()
    df["OnTime_Flag"] = np.where(
        df["Delivery_Status"].eq("On-Time"), 1,
        np.where(df["Delivery_Status"].eq("Late"), 0, np.nan)
    )
    df["Late_Flag"] = np.where(
        df["Delivery_Status"].eq("Late"), 1,
        np.where(df["Delivery_Status"].eq("On-Time"), 0, np.nan)
    )
    return df

def order_status(df: pd.DataFrame):
    """Determine the status of each order (Pending, Completed, Cancelled, etc.)."""
    df = df.copy()
    order = pd.Series(np.nan, index=df.index, dtype="object")

    rejected = df.get("Status").eq("Rejected") if "Status" in df.columns else pd.Series(False, index=df.index)
    approved = df.get("Status").eq("Approved") if "Status" in df.columns else pd.Series(False, index=df.index)

    has_arrived = approved & df["Date_Arrived"].notna()
    not_arrived = approved & df["Date_Arrived"].isna()

    order.loc[rejected] = "Cancelled"
    if "Status_Arrived" in df.columns:
        order.loc[has_arrived & (df["Status_Arrived"] == "Completed")] = "Completed"
        order.loc[has_arrived & (df["Status_Arrived"] == "Partial")] = "Partial"

    order.loc[not_arrived] = "Pending"
    df["Order_Status"] = order
    return df

def categorize_lead_time(df: pd.DataFrame):
    """Categorize lead times into buckets."""
    df = df.copy()
    cat = pd.Series(np.nan, index=df.index, dtype="object")
    cat.loc[(df["Lead_Time"] >= 1) & (df["Lead_Time"] <= 3)] = "1-3"
    cat.loc[(df["Lead_Time"] > 3) & (df["Lead_Time"] <= 7)] = "4-7"
    cat.loc[(df["Lead_Time"] > 7) & (df["Lead_Time"] <= 14)] = "8-14"
    cat.loc[df["Lead_Time"] > 14] = ">14"
    df["Lead_Category"] = cat
    return df

def df_preview_table(df: pd.DataFrame, max_rows=10, max_cols=12):
    """Generate a preview of the dataframe with limited rows and columns."""
    if df is None or df.empty:
        return ft.Text("No data to preview.")
    cols = list(df.columns)[:max_cols]
    view = df[cols].head(max_rows).astype(str)

    return ft.DataTable(
        columns=[ft.DataColumn(ft.Text(c, weight=ft.FontWeight.BOLD)) for c in cols],
        rows=[
            ft.DataRow(cells=[ft.DataCell(ft.Text(view.iloc[r, c])) for c in range(len(cols))])
            for r in range(len(view))
        ],
        heading_row_height=42,
        data_row_min_height=36,
        data_row_max_height=44,
        column_spacing=14,
    )

def export_to_excel(data_dict, output_path: str):
    """Export data to Excel format."""
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, df in data_dict.items():
            if df is None:
                continue
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)

def export_to_csv_folder(data_dict, folder_path: str):
    """Export data to CSV format in a specified folder."""
    Path(folder_path).mkdir(parents=True, exist_ok=True)
    for name, df in data_dict.items():
        if df is None:
            continue
        df.to_csv(os.path.join(folder_path, f"{name}.csv"), index=False)

def apply_column_mapping(df: pd.DataFrame, mapping: dict, keep_only_mapped: bool = True):
    """Apply the column mappings (rename columns based on user selection)."""
    df = df.copy()

    # Rename columns based on the provided mapping
    ren = {actual: canonical for canonical, actual in mapping.items()
           if actual and actual in df.columns}

    if keep_only_mapped:
        keep_cols = list(dict.fromkeys(ren.keys()))  # Keep only mapped columns
        df = df[keep_cols]

    df = df.rename(columns=ren)  # Rename columns to canonical names
    return df

# ---------------------------
# FLET APP
# ---------------------------
def app(page: ft.Page):
    """Main Flet application for the procurement data processing wizard."""
    page.title = "Procurement Data Processing (AutoMap + Override Wizard)"
    page.scroll = ft.ScrollMode.AUTO

    # State initialization
    state = {
        "file_path": None,
        "sheets_raw": None,
        "sheet_names": [],
        "step_idx": 0,
        "sheet_choice": {t: None for t in TABLE_STEPS},
        "col_choice": {t: {} for t in TABLE_STEPS},
        "tables": {},
        "export_payload": None,
        "map_scores": {t: {} for t in TABLE_STEPS},
    }

    # UI components
    log = ft.TextField(value="Ready.\n", multiline=True, read_only=True, expand=True, min_lines=8)
    status = ft.Text("Status: idle", size=14)
    progress = ft.ProgressRing(visible=False)

    export_format = ft.Dropdown(
        label="Export format",
        options=[ft.dropdown.Option("Excel (.xlsx)"), ft.dropdown.Option("CSV folder")],
        value="Excel (.xlsx)",
        width=200,
    )

    wizard_title = ft.Text("", size=18, weight=ft.FontWeight.BOLD)
    wizard_help = ft.Text("", size=12, italic=True)
    sheet_dropdown = ft.Dropdown(label="Select sheet for this table", options=[], width=360)

    mapping_area = ft.Column(spacing=8)
    preview_area = ft.Container(content=ft.Text("No preview yet."), padding=10)

    btn_back = ft.ElevatedButton("Back")
    btn_next = ft.ElevatedButton("Next")
    btn_process = ft.ElevatedButton("Process Data", disabled=True)
    btn_export = ft.ElevatedButton("Export", disabled=True)

    btn_automap_step = ft.OutlinedButton("AutoMap this step")
    btn_automap_all = ft.OutlinedButton("AutoMap ALL tables")

    # Write messages to the log
    def write_log(msg: str):
        log.value += msg.rstrip() + "\n"
        page.update()

    # Show completion message popup
    def show_done(msg: str):
        page.snack_bar = ft.SnackBar(ft.Text(msg), open=True)
        page.update()

    # Check if all required columns are mapped for a given table
    def required_mapped(table):
        req = REQUIRED_COLS.get(table, [])
        if not req:
            return True
        if not state["sheet_choice"].get(table):
            return False
        for r in req:
            if not state["col_choice"][table].get(r):
                return False
        return True

    # Refresh the UI for the current step
    def refresh_step_ui():
        t = TABLE_STEPS[state["step_idx"]]
        wizard_title.value = f"Step {state['step_idx']+1}/{len(TABLE_STEPS)}: Map {t}"
        wizard_help.value = "AutoMap will pre-fill best guesses. You can override any dropdown. Export keeps ONLY mapped columns."

        sheet_dropdown.options = [ft.dropdown.Option(s) for s in state["sheet_names"]]
        sheet_dropdown.value = state["sheet_choice"].get(t)

        mapping_area.controls.clear()

        chosen_sheet = sheet_dropdown.value
        if not chosen_sheet:
            mapping_area.controls.append(ft.Text("Select a sheet to see its columns and AutoMap."))
            preview_area.content = ft.Text("No preview yet.")
        else:
            df = state["sheets_raw"][chosen_sheet]
            cols = list(df.columns)

            required = REQUIRED_COLS.get(t, [])

            if required:
                mapping_area.controls.append(
                    ft.Row(
                        [
                            ft.Text("Required fields", weight=ft.FontWeight.BOLD),
                            ft.Container(width=10),
                            ft.Text("(confidence shown)", size=12, italic=True),
                        ],
                        wrap=True
                    )
                )

                # Add dropdowns for each required field to allow manual overrides
                for canon_col in required:
                    current_val = state["col_choice"][t].get(canon_col)
                    sc = state["map_scores"].get(t, {}).get(canon_col, 0.0)
                    conf_txt = f"{int(round(sc*100))}%" if sc else "—"

                    dd = ft.Dropdown(
                        label=f"{canon_col} (required)  •  Auto confidence: {conf_txt}",
                        options=[ft.dropdown.Option(c) for c in cols],
                        value=current_val,
                        width=520,
                    )

                    # Update the selected column when changed
                    def make_on_change(canon):
                        def _on_change(e):
                            state["col_choice"][t][canon] = e.control.value
                            page.update()
                        return _on_change

                    dd.on_change = make_on_change(canon_col)
                    mapping_area.controls.append(dd)
            else:
                mapping_area.controls.append(ft.Text("No required fields configured for this table."))

            preview_area.content = ft.Column(
                [
                    ft.Text(f"Preview of '{chosen_sheet}' (rows: {len(df):,})", weight=ft.FontWeight.BOLD),
                    ft.Container(content=df_preview_table(df), border=ft.border.all(1), padding=8),
                ],
                scroll=ft.ScrollMode.AUTO,
            )

        # Update navigation buttons
        btn_back.disabled = state["step_idx"] == 0
        btn_next.disabled = state["step_idx"] == len(TABLE_STEPS) - 1
        btn_process.disabled = not all(required_mapped(tn) for tn in TABLE_STEPS)
        page.update()

    # File picker to upload an Excel file
    def on_open_result(e: ft.FilePickerResultEvent):
        if not e.files:
            status.value = "Status: no file selected"
            write_log("No file selected.")
            page.update()
            return

        fp = e.files[0].path
        state["file_path"] = fp
        status.value = f"Status: selected {fp}"
        write_log(f"Selected file: {fp}")

        try:
            progress.visible = True
            page.update()

            # Import sheets from the selected Excel file
            state["sheets_raw"] = import_excel_sheets(fp)
            state["sheet_names"] = list(state["sheets_raw"].keys())
            write_log(f"Loaded {len(state['sheet_names'])} sheets: {state['sheet_names']}")

            # Reset the wizard state for mapping columns
            state["step_idx"] = 0
            state["sheet_choice"] = {t: None for t in TABLE_STEPS}
            state["col_choice"] = {t: {} for t in TABLE_STEPS}
            state["map_scores"] = {t: {} for t in TABLE_STEPS}

            refresh_step_ui()
            status.value = "Status: uploaded (start mapping wizard)"
        except Exception as ex:
            status.value = "Status: failed to load file"
            write_log(f"❌ Could not load Excel: {ex}")
        finally:
            progress.visible = False
            page.update()

    open_picker = ft.FilePicker(on_result=on_open_result)
    page.overlay.append(open_picker)

    def on_save_result(e: ft.FilePickerResultEvent):
        """Handle saving the exported data."""
        if not getattr(e, "path", None):
            write_log("Export cancelled (no save path).")
            status.value = "Status: export cancelled"
            page.update()
            return

        try:
            payload = state["export_payload"]
            if payload is None:
                status.value = "Status: nothing to export"
                write_log("❌ Nothing to export.")
                page.update()
                return

            # Export to Excel or CSV based on user's choice
            if export_format.value == "Excel (.xlsx)":
                out_path = e.path
                if not out_path.lower().endswith(".xlsx"):
                    out_path += ".xlsx"
                export_to_excel(payload, out_path)
                status.value = f"Status: exported Excel to {out_path}"
                write_log(f"✅ Exported Excel: {out_path}")
                show_done("✅ Export completed successfully (Excel).")
            else:
                base = os.path.splitext(e.path)[0]
                folder = base + "_csv"
                export_to_csv_folder(payload, folder)
                status.value = f"Status: exported CSVs to {folder}"
                write_log(f"✅ Exported CSV folder: {folder}")
                show_done("✅ Export completed successfully (CSV folder).")

        except Exception as ex:
            status.value = "Status: export failed"
            write_log(f"❌ Export failed: {ex}")

        page.update()

    save_picker = ft.FilePicker(on_result=on_save_result)
    page.overlay.append(save_picker)

    # Events for uploading, sheet selection, and navigation
    def on_upload_click(_):
        open_picker.pick_files(allow_multiple=False, allowed_extensions=["xlsx", "xls"])

    def on_sheet_change(e):
        t = TABLE_STEPS[state["step_idx"]]
        selected_sheet = e.control.value
        state["sheet_choice"][t] = selected_sheet

        # Update the alias in the JSON file (only if the sheet name is not already in the aliases)
        if selected_sheet and selected_sheet not in ALIASES.get(t, []):
            ALIASES[t] = ALIASES.get(t, []) + [selected_sheet]

            # Save the updated aliases back to the JSON file
            with open('aliases.json', 'w') as f:
                json.dump(ALIASES, f, indent=4)

            write_log(f"Updated aliases for {t}: {ALIASES[t]}")

        # Reset column choices when a new sheet is chosen
        state["col_choice"][t] = {}

        # Auto map columns after sheet selection
        if selected_sheet:
            df = state["sheets_raw"][selected_sheet]
            mapping, scores = auto_map_required(t, df)
            for canon, actual in mapping.items():
                if not state["col_choice"][t].get(canon):
                    state["col_choice"][t][canon] = actual
            state["map_scores"][t] = scores
            write_log(f"🔎 AutoMapped {t} from sheet '{selected_sheet}' (you can override).")

        refresh_step_ui()

    sheet_dropdown.on_change = on_sheet_change

    def on_back(_):
        state["step_idx"] = max(0, state["step_idx"] - 1)
        refresh_step_ui()

    def on_next(_):
        state["step_idx"] = min(len(TABLE_STEPS) - 1, state["step_idx"] + 1)
        refresh_step_ui()

    def on_clear_mapping_click(_):
        t = TABLE_STEPS[state["step_idx"]]
        state["col_choice"][t] = {}
        state["map_scores"][t] = {}
        write_log(f"✅ Mappings cleared for {t}.")
        refresh_step_ui()

    btn_clear_mapping = ft.OutlinedButton("Clear Mapping", on_click=on_clear_mapping_click)

    btn_back.on_click = on_back
    btn_next.on_click = on_next

    def on_automap_step(_):
        t = TABLE_STEPS[state["step_idx"]]
        chosen_sheet = state["sheet_choice"].get(t)
        if not chosen_sheet:
            write_log(f"⚠️ Select a sheet first before AutoMap for {t}.")
            return

        df = state["sheets_raw"][chosen_sheet]
        mapping, scores = auto_map_required(t, df)

        for canon, actual in mapping.items():
            if not state["col_choice"][t].get(canon):
                state["col_choice"][t][canon] = actual
        state["map_scores"][t] = scores

        write_log(f"✅ AutoMapped step: {t}. Review and override if needed.")
        refresh_step_ui()

    btn_automap_step.on_click = on_automap_step

    def on_automap_all(_):
        if not state["sheets_raw"]:
            write_log("⚠️ Upload a file first.")
            return

        for t in TABLE_STEPS:
            if state["sheet_choice"].get(t):
                chosen_sheet = state["sheet_choice"][t]
            else:
                best_sheet, best_sc = None, 0.0
                for s in state["sheet_names"]:
                    sc = similarity(s, t)
                    if sc > best_sc:
                        best_sc = sc
                        best_sheet = s
                chosen_sheet = best_sheet
                state["sheet_choice"][t] = chosen_sheet

            if chosen_sheet:
                df = state["sheets_raw"][chosen_sheet]
                mapping, scores = auto_map_required(t, df)
                for canon, actual in mapping.items():
                    if not state["col_choice"][t].get(canon):
                        state["col_choice"][t][canon] = actual
                state["map_scores"][t] = scores

        write_log("✅ AutoMapped ALL tables (sheet guess + column guess). Please review each step.")
        refresh_step_ui()

    btn_automap_all.on_click = on_automap_all

    def on_process_click(_):
        try:
            progress.visible = True
            status.value = "Status: processing..."
            page.update()

            def get_table(table_name):
                sheet = state["sheet_choice"].get(table_name)
                if not sheet:
                    return None
                df = state["sheets_raw"][sheet]
                mapped = apply_column_mapping(df, state["col_choice"][table_name], keep_only_mapped=True)

                wanted = [c for c in REQUIRED_COLS.get(table_name, []) if c in mapped.columns]
                mapped = mapped[wanted]
                return mapped

            po_df = get_table("PurchaseOrder")
            grn_df = get_table("GRN")
            inv_df = get_table("Invoices")
            item_df = get_table("Item")
            supp_df = get_table("Suppliers")
            pol_df = get_table("PurchaseOrderLine")

            validate_columns(po_df, REQUIRED_COLS["PurchaseOrder"], "PurchaseOrder")
            validate_columns(grn_df, REQUIRED_COLS["GRN"], "GRN")
            validate_columns(inv_df, REQUIRED_COLS["Invoices"], "Invoices")
            validate_columns(item_df, REQUIRED_COLS["Item"], "Item")
            validate_columns(supp_df, REQUIRED_COLS["Suppliers"], "Suppliers")
            validate_columns(pol_df, REQUIRED_COLS["PurchaseOrderLine"], "PurchaseOrderLine")

            defect_df = defect_rate(grn_df)
            keep_defect = [c for c in ["GRN_No", "PO_ID", "Defect_Rate"] if c in defect_df.columns]
            defect_df = defect_df[keep_defect]

            grn_latest = filter_latest_grn(grn_df)

            merged = join_po_grn(po_df, grn_latest)
            merged = compute_lead_time(merged)
            merged = on_time_late(merged)
            merged = order_status(merged)
            merged = add_flags(merged)
            merged = categorize_lead_time(merged)

            keep_merged = [
                "PO_ID",
                "Lead_Time",
                "Delivery_Status",
                "Order_Status",
                "OnTime_Flag",
                "Late_Flag",
                "Lead_Category",
            ]
            keep_merged = [c for c in keep_merged if c in merged.columns]
            merged = merged[keep_merged]

            state["tables"] = {
                "Merged_PO_GRN": merged,
                "Defect_Rate": defect_df,
                "PurchaseOrder": po_df,
                "GRN_Latest": grn_latest,
                "Invoices": inv_df,
                "Item": item_df,
                "Suppliers": supp_df,
                "PurchaseOrderLine": pol_df,
            }
            state["export_payload"] = state["tables"]

            preview_area.content = ft.Column(
                [
                    ft.Text(f"Merged_PO_GRN (rows: {len(merged):,})", weight=ft.FontWeight.BOLD),
                    ft.Container(content=df_preview_table(merged), border=ft.border.all(1), padding=8),
                ],
                scroll=ft.ScrollMode.AUTO,
            )

            btn_export.disabled = False
            status.value = "Status: processed successfully"
            write_log("✅ Processed successfully. Export is enabled now.")

        except Exception as ex:
            status.value = "Status: processing failed"
            write_log(f"❌ Processing failed: {ex}")
        finally:
            progress.visible = False
            page.update()

    btn_process.on_click = on_process_click


    def on_export_click(_):
        if not state.get("export_payload"):
            write_log("❌ No processed data to export.")
            return

        # Get the directory where the script is running
        app_directory = Path(__file__).parent
    
        # Define the export file path
        export_file_path = app_directory / "Processed_Procurement.xlsx"

        # Export to Excel
        export_to_excel(state["export_payload"], export_file_path)

        write_log(f"✅ Exported Excel to {export_file_path}")
        status.value = f"Status: Exported to {export_file_path}"

        page.update()

    btn_export.on_click = on_export_click

    # Layout components
    page.add(
        ft.Row(
            [
                ft.ElevatedButton("Upload Excel File", on_click=on_upload_click),
                progress,
                export_format,
                ft.Container(width=10),
                btn_automap_all,
            ],
            wrap=True
        ),
        status,
        ft.Divider(),
        ft.Text("Mapping Wizard", weight=ft.FontWeight.BOLD),
        wizard_title,
        wizard_help,
        ft.Row([sheet_dropdown, btn_automap_step], wrap=True),
        ft.Container(content=mapping_area, padding=10, border=ft.border.all(1)),
        ft.Row([btn_back, btn_next, btn_clear_mapping, btn_process, btn_export], wrap=True),
        ft.Divider(),
        ft.Text("Preview", weight=ft.FontWeight.BOLD),
        preview_area,
        ft.Divider(),
        ft.Text("Log", weight=ft.FontWeight.BOLD),
        log,
    )

    # Initial refresh for the UI
    refresh_step_ui()

# Run the Flet app
ft.app(target=app)
