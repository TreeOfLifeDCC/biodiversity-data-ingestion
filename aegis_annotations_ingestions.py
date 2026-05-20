import json
import csv
import urllib.request
from pathlib import Path
from collections import defaultdict

JSON_URL = "https://ftp.ebi.ac.uk/pub/ensemblorganisms/species.json"
JSON_CACHE = Path("species.json")
TSV_PATH = Path("results.tsv")
OUT_TSV = Path("results_enriched.tsv")
OUT_JSON = Path("results_filtered.json")

# 1. Load JSON (cache locally so you don't re-download)
if not JSON_CACHE.exists():
    print(f"Downloading {JSON_URL} ...")
    urllib.request.urlretrieve(JSON_URL, JSON_CACHE)

with JSON_CACHE.open() as f:
    payload = json.load(f)

species = payload["species"]   # <-- unwrap once, use everywhere below
print(f"JSON last_updated: {payload.get('last_updated')}")

# 2. Build chain -> [(species_name, full_gca, assembly_record), ...]
# A chain can map to multiple versioned assemblies, so use a list.
chain_index = defaultdict(list)
for sp_name, sp_record in species.items():
    for full_gca, asm_record in sp_record.get("assemblies", {}).items():
        chain = full_gca.split(".")[0]  # "GCA_964058905.1" -> "GCA_964058905"
        chain_index[chain].append((sp_name, full_gca, asm_record))

print(f"Indexed {sum(len(v) for v in chain_index.values())} assemblies "
      f"across {len(chain_index)} chains")

# 3. Walk the TSV and collect matches
matched_rows = []      # for enriched TSV
filtered_json = {}     # for filtered JSON (species -> full record subset)
unmatched = []

with TSV_PATH.open() as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
        chain = row.get("gca_chain", "").strip()
        if not chain:
            continue
        hits = chain_index.get(chain)
        if not hits:
            unmatched.append(chain)
            continue
        for sp_name, full_gca, asm_record in hits:
            enriched = dict(row)
            enriched["matched_species"] = sp_name
            enriched["matched_gca"] = full_gca
            enriched["scientific_name"] = species[sp_name].get("scientific_name")
            enriched["taxid"] = species[sp_name].get("taxid")
            matched_rows.append(enriched)

            # Build trimmed JSON keeping only matched assemblies per species
            if sp_name not in filtered_json:
                filtered_json[sp_name] = {
                    k: v for k, v in species[sp_name].items() if k != "assemblies"
                }
                filtered_json[sp_name]["assemblies"] = {}
            filtered_json[sp_name]["assemblies"][full_gca] = asm_record

# 4. Write outputs
if matched_rows:
    fieldnames = list(matched_rows[0].keys())
    with OUT_TSV.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        w.writeheader()
        w.writerows(matched_rows)

with OUT_JSON.open("w") as f:
    json.dump(filtered_json, f, indent=2)

print(f"Matched rows: {len(matched_rows)}")
print(f"Unmatched chains: {len(unmatched)}")
if unmatched[:5]:
    print("First few unmatched:", unmatched[:5])
print(f"Wrote {OUT_TSV} and {OUT_JSON}")
