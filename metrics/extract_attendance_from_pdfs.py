import os
import re
import pdfplumber
import pandas as pd

PDF_DIR = "raw_data/dominican_republic/2025"
OUTPUT_CSV = "processed_data/dominican_republic_attendance_2025.csv"
os.makedirs("processed_data", exist_ok=True)

def extract_session_date(text):
    match = re.search(r"Fecha Sesión:\s*(\d{2}/\d{2}/\d{4})", text)
    return match.group(1) if match else None

def parse_attendance_lines(lines, session_date, source_file):
    records = []
    start_parsing = False

    # Longer matches first
    status_keywords = sorted([
        "Presente Incorporado",
        "En Representación",
        "Representación de la",  # Edge-case alias
        "Presente",
        "Ausente",
        "Excusa",
        "Licencia",
        "Incorporado"
    ], key=len, reverse=True)

    for line in lines:
        line = line.strip()

        if not start_parsing:
            if "# Funcionario Asistensia" in line or "Funcionario Asistencia" in line:
                start_parsing = True
            continue

        if re.match(r"^\d+\s+", line):
            line = re.sub(r"^\d+\s+", "", line)
            found_status = None

            for keyword in status_keywords:
                if keyword in line:
                    found_status = keyword
                    break

            if not found_status:
                print(f"[SKIP] No status found in line: {line}")
                continue

            # Normalize alias
            if found_status == "Representación de la":
                found_status = "En Representación"

            parts = line.split(found_status)
            name = parts[0].strip()
            after_status = parts[1].strip() if len(parts) > 1 else ""
            arrival_time = after_status if re.search(r"\d{1,2}:\d{2}", after_status) else ""

            records.append({
                "name": name,
                "status": found_status,
                "arrival_time": arrival_time,
                "session_date": session_date,
                "source_file": source_file
            })

    return records

def main():
    all_records = []

    for filename in os.listdir(PDF_DIR):
        if not filename.endswith(".pdf"):
            continue

        pdf_path = os.path.join(PDF_DIR, filename)
        print(f"[READ] {filename}")

        with pdfplumber.open(pdf_path) as pdf:
            first_page_text = pdf.pages[0].extract_text()
            session_date = extract_session_date(first_page_text)

            lines = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    lines.extend(text.split("\n"))

            records = parse_attendance_lines(lines, session_date, filename)
            all_records.extend(records)

    df = pd.DataFrame(all_records)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"[DONE] Saved {len(df)} rows to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
