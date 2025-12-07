from collections import defaultdict

# Example input:
data = """
Anne: Metallica, The_Doors, Black_Sabbath
John: The_Beatles, The_Doors, Metallica, Pink_Floyd
Kathy: U2, Guns_n_Roses, Led_Zeppelin
Jamie: Radiohead
Ashok: Guns_n_Roses, U2, Pink_Floyd, The_Doors
Sara: Blink_182, Iron_Maiden, The_Doors
"""

# Dictionary: band --->> list of colleagues
band_to_people = defaultdict(list)

# Parse the input
for line in data.strip().split("\n"):
    if ":" not in line:
        continue
    
    name, bands = line.split(":", 1)
    name = name.strip()
    
    band_list = [b.strip() for b in bands.split(",") if b.strip()]
    
    for band in band_list:
        band_to_people[band].append(name)

# Count likes for each band from colleagues
band_count = {band: len(people) for band, people in band_to_people.items()}

# ---- PART A.1: Top 2 most liked bands (including ties) ----
# Get unique sorted counts (descending)

unique_counts = sorted(set(band_count.values()), reverse=True)

top_counts = unique_counts[:2]  # Top 2 distinct positions

top_bands = [band for band, count in band_count.items() if count in top_counts]

print("Top bands:\n")
for band in sorted(top_bands, key=lambda b: (-band_count[b], b)):
    print(band)

# ---- PART A.2: Band with comma-separated colleagues ----

print("\nBand and colleagues:\n")

for band, people in sorted(band_to_people.items()):
    print(f"{band}: {', '.join(sorted(people))}")
