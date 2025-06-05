import pandas as pd
import os
import re
import unicodedata

INPUT_CSV = "processed_data/dominican_republic_attendance_2025.csv"
OUTPUT_CSV = "processed_data/cleaned_dominican_republic_attendance_2025.csv"

SURNAME_PARTICLES = {"de", "del", "de la", "la", "los", "y"}

def normalize_text(text):
    if not isinstance(text, str):
        return text
    return unicodedata.normalize("NFKD", text).encode("utf-8", "ignore").decode("utf-8")

def split_name(full_name):
    if not isinstance(full_name, str):
        return "", ""

    tokens = full_name.split()
    if len(tokens) < 2:
        return full_name, ""

    last_name_parts = []
    first_name_parts = []
    i = 0

    while i < len(tokens):
        token = tokens[i].lower()

        # Check for 2-word particles like "de la"
        if i + 1 < len(tokens):
            next_pair = f"{token} {tokens[i+1].lower()}"
            if next_pair in SURNAME_PARTICLES:
                last_name_parts.append(f"{tokens[i]} {tokens[i+1]}")
                i += 2
                continue

        # Check for 1-word particle
        if token in SURNAME_PARTICLES:
            last_name_parts.append(tokens[i])
            i += 1
            continue

        # Once a token is not a particle, assume rest is first name(s)
        break

    # Assume the first two tokens are last names (or 1 if short name)
    if not last_name_parts:
        last_name_parts = tokens[:2] if len(tokens) >= 3 else tokens[:1]
    first_name_parts = tokens[len(last_name_parts):]

    return " ".join(last_name_parts), " ".join(first_name_parts)

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"[ERROR] Input file not found: {INPUT_CSV}")
        return

    df = pd.read_csv(INPUT_CSV)

    # Normalize encoding and trim whitespace
    df["name"] = df["name"].fillna("").apply(lambda x: normalize_text(x.strip()))
    df["status"] = df["status"].fillna("").apply(lambda x: normalize_text(x.strip()))
    df["arrival_time"] = df["arrival_time"].fillna("").apply(lambda x: normalize_text(x.strip()))

    # Split into name components
    df[["last_name", "first_name"]] = df["name"].apply(lambda x: pd.Series(split_name(x)))

    # Optional: reorder columns
    columns = ["first_name", "last_name", "status", "arrival_time", "session_date", "source_file"]
    df = df[columns]

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"[DONE] Saved cleaned data to {OUTPUT_CSV} with {len(df)} rows")

if __name__ == "__main__":
    main()
