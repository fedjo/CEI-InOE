import pandas as pd
import xlrd
import re

xls_path = "Milking Performance Parlour - Animal_Kyriakos Kallenos.xls"

# Read all rows as raw data (no header)
df = pd.read_excel(xls_path, header=None, engine='xlrd')

results = []
current_date = None
milk_sum = 0
animal_sum = 0
session_count = 0

for idx, row in df.iterrows():
    # Detect date header (e.g., 'Date 27/03/2026')
    first_cell = str(row[0])
    m = re.match(r"Date\s+(\d{2}/\d{2}/\d{4})", first_cell)
    if m:
        # Save previous date's result
        if current_date and session_count > 0:
            avg_animals = animal_sum / session_count
            yield_per_cow = milk_sum / avg_animals if avg_animals else 0
            results.append({
                'date': current_date,
                'avg_animals': avg_animals,
                'total_milk': milk_sum,
                'yield_per_cow': yield_per_cow
            })
        # Start new date
        current_date = m.group(1)
        milk_sum = 0
        animal_sum = 0
        session_count = 0
        continue
    # Skip summary/empty rows
    if first_cell.strip() in ('', 'T') or pd.isna(row[1]):
        continue
    # Try to parse animal count and milk yield
    try:
        animals = float(row[5]) if not pd.isna(row[2]) else 0
        milk = float(row[9]) if not pd.isna(row[10]) else 0
        animal_sum += animals
        milk_sum += milk
        session_count += 1
        print(f"Date: {current_date}, Animals: {animals}, Milk: {milk} kg")
    except Exception:
        continue
# Save last date
if current_date and session_count > 0:
    avg_animals = (animal_sum / 2) / (session_count -1)
    yield_per_cow = milk_sum / (avg_animals)  if avg_animals else 0
    results.append({
        'date': current_date,
        'avg_animals': avg_animals,
        'total_milk': milk_sum / 2,
        'yield_per_cow': yield_per_cow
    })

# Print results
for r in results:
        print(f"Date: {r['date']}, Avg Animals: {r['avg_animals']:.2f}, Total Milk: {r['total_milk']:.2f}, Yield per Cow: {r['yield_per_cow']:.2f} kg")
