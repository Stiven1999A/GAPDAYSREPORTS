"""
Utility functions for data processing and formatting
"""
import os
import zipfile
from pathlib import Path
import re
from typing import Set, Optional

def hours_to_hhmm(x):
    """Format y-axis as hh:mm (hours:minutes)"""
    h = int(x)
    m = int((x - h) * 60)
    if m == 60:
        h += 1
        m = 0
    return f"{h:02d}h:{m:02d}m"
 
def zip_folder(folder_path, output_path):
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                # Preserve folder structure inside the zip
                arcname = os.path.relpath(file_path, folder_path)
                zipf.write(file_path, arcname)

def eeids_reports_cache(
    output_folder_path: Optional[str] = None,
    reports_folder_name: str = "zero_prod_reports",
    eeid_pattern: str = r"\b([A-Z]\d{5})\b",
    verbose: bool = True,
) -> Set[str]:
    """
    Return the set of EEIDs that already have a 'zero productivity' report generated.

    Parameters
    ----------
    output_folder_path : Optional[str]
        Base output path that contains the reports folder. If None, uses a default.
    reports_folder_name : str
        Name of the folder under `output_folder_path` where zero-prod reports live.
    eeid_pattern : str
        Regex used to extract EEIDs from filenames. Default matches e.g. 'A28465'.
        Adjust if your IDs follow a different format.
    verbose : bool
        If True, prints helpful diagnostics.

    Returns
    -------
    Set[str]
        A set of unique EEIDs discovered from the filenames in the reports folder.

    Notes
    -----
    - This function does *not* create folders; it only reads what exists.
    - EEID extraction is done via regex across each filename. If your files
      are named like 'ZeroProd_A28465_2026-01-15.pdf', this will capture 'A28465'.
    - If multiple EEIDs appear in a single filename, all matches are included.
    """
    # Sensible default if not provided
    if output_folder_path is None:
        output_folder_path = "/Users/Estiben.Gonzalez/Downloads/Daily_AT_Report/GapDaysReports/app/data/output/"

    base_path = Path(output_folder_path).expanduser().resolve()
    reports_path = base_path / reports_folder_name

    if not base_path.exists():
        if verbose:
            print(f"[{reports_folder_name}] Base path does not exist: {base_path}")
        return set()

    if not reports_path.exists():
        if verbose:
            print(f"[{reports_folder_name}] Reports folder not found: {reports_path}")
        return set()

    # Gather files (ignore subdirectories)
    files = [p for p in reports_path.iterdir() if p.is_file()]
    if not files:
        if verbose:
            print(f"[zero_prod_cache] Reports folder is empty: {reports_path}")
        return set()

    # Compile regex once
    eeid_re = re.compile(eeid_pattern)

    eeids_done: Set[str] = set()
    for f in files:
        # Search in the filename (without extension) and also full name as fallback
        name_no_ext = f.stem
        matches = eeid_re.findall(name_no_ext) or eeid_re.findall(f.name)
        for m in matches:
            eeids_done.add(m)

    if verbose:
        print(f"[zero_prod_cache] Base:    {base_path}")
        print(f"[zero_prod_cache] Reports: {reports_path}")
        print(f"[zero_prod_cache] Found {len(eeids_done)} EEIDs: {sorted(eeids_done)}")

    return eeids_done

if __name__ == "__main__":
    input_folder_path = '/Users/Estiben.Gonzalez/Downloads/Daily_AT_Report/GapDaysReports/app/data/output/gap_reports'
    output_folder_path = '/Users/Estiben.Gonzalez/Downloads/Daily_AT_Report/GapDaysReports/app/data/output/gap_reports_zip.zip'
    zip_folder(input_folder_path, output_folder_path)
    print("Successfully compressed")
