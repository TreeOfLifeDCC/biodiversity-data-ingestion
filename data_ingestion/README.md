# Dataflow Flex Templates for the Biodiversity pipelines

The entry point is `main.py`. This calls `launcher.py`, which contains the `launch_pipeline` function that runs the pipeline via the `--pipeline` command line.
(Note: the GCS and BigQuery steps in the pipelines have not yet been tested with the Flex template.)

**Implemented pipelines:**

* Taxonomy
* Occurrences
* Cleaning occurrences
* Spatial annotation
* Range size estimation
* Data provenance

## How to run this repo

```bash
git clone git@github.com:TreeOfLifeDCC/biodiversity-data-ingestion.git
```
Please replace below the `<xxxx>` with the actual value.

### 1. Build docker image

To test this image locally, you will need to include the data directory. This contains all the raster and vector layers required for the spatial cleaning and annotation pipelines.
Download it from here: https://drive.google.com/drive/folders/1AQ-vggPieKmCjHA65cuvj7O1Q6nqDQab?usp=sharing


```bash
docker build -f Dockerfile -t europe-west2-docker.pkg.dev/<projectid>/biodiversity-images/<image_name>:<your_tag> .
```
### 2. Test locally

**Taxonomy pipeline**

```bash
docker run --rm \                                           
  -v $(pwd)/out:/app/out \
  --entrypoint python \
  <docker-image> \
  /template/main.py \
  --pipeline taxonomy \
  --host <host> \
  --user <user> \
  --password <password> \
  --index data_portal \
  --output "/app/out/flex_taxonomy" \
  --size 10 \
  --pages 1 \
  --sleep 0.25 \
  --direct_num_workers=1
  ```
*Output*:
* `flex_taxonomy_tocheck.jsonl` (0 rows)
* `flex_taxonomy_validated.jsonl` (10 rows)

**Occurrences pipeline**
  
```bash
docker run --rm \
-v "$PWD/out":/app/out \
--entrypoint python \
<docker-image> \
/template/main.py \
--pipeline occurrences \
--validated_input "/app/out/flex_taxonomy_validated.jsonl" \
--output_dir /app/out/occurrences \
--limit 100 \
--runner DirectRunner \
--direct_num_workers=1
````

*Output*:
* Directory `occurrences/` with:
* 10 files with 100 occurrences each: `occ_scientific_name.jsonl`. Species occurrences from GBIF.
* One file: `summary_occ_download.jsonl`. Summary of the species occurrences search and download.

**Cleaning occurrence data pipeline**

```bash
docker run --rm \
  -v "$PWD/out":/app/out \
  --entrypoint python \
  <docker-image> \
  /template/main.py \
  --pipeline cleaning_occs \
  --input_glob "/template/data/occurrences/occ_*.jsonl" \
  --output_dir "/app/out/occurrences/clean" \
  --land_shapefile "/template/data/spatial_processing/ne_10m_land/ne_10m_land.shp" \
  --centroid_shapefile "/template/data/spatial_processing/ne_10m_admin_0_label_points/ne_10m_admin_0_label_points.shp" \
  --max_uncertainty 5000 \
  --min_uncertainty 1000 \
  --max_centroid_dist 5000 \
  --runner DirectRunner \
  --output_consolidated "/app/out/occurrences/clean/all_species"
```
*Output*:
* Directory `occurrences/clean/` with:
* 10 files with cleaned occurrences: `occ_scientific_name.jsonl`. The number of occurrences per file is variable.
* One file: `all_species.jsonl`. A consolidated file of cleaned species occurrences if `--output_consolidated` was provided. 

**Spatial annotation pipeline**

```bash
docker run --rm \
  -v "$PWD/out":/app/out \
  --entrypoint python \
  <docker-image> \
  /template/main.py \
  --pipeline spatial_annotation \
  --input_occs "/app/out/occurrences/clean/occ_*.jsonl" \
  --climate_dir "/template/data/climate" \
  --biogeo_vector "/template/data/bioregions/Ecoregions2017.zip" \
  --annotated_output "/app/out/spatial/annotated" \
  --summary_output "/app/out/spatial/summary" \
  --runner DirectRunner
```
*Output*:
* Directory `spatial/` with:
* One file `annotated.jsonl` which contains spatial annotations per species occurrence.
* One file `summary.jsonl` which contains descriptive statistics over climate data and unique values for biogeographical regions, extracted using occurrence data.

**Range size estimation pipeline**

```bash
docker run --rm \
  -v "$PWD/out":/app/out \
  --entrypoint python \
  occ_pipeline:test \
  /template/main.py \
  --pipeline range_estimation \
  --input_glob "/app/out/occurrences/occ_*.jsonl"
  --bq_table <project.dataset.table>
  --bq_schema <gs://<my-bucket>/schema.json>
  --temp_location <gs://<my-bucket>/tmp>
```
NOTE: Awaiting to test directly in the cloud.

*Output*:
* BiqQuery table with range size data and one row per species. 


```bash
docker run --rm \
  -v "$PWD/out":/app/out \
  --entrypoint python \
  occ_pipeline:test \
  /template/main.py \
  --pipeline data_provenance \
  --host <host> \
  --user <user> \
  --password <password> \
  --index <index> \
  --output "/app/out/data_provenance/results" \
  --page_size 10 \
  --max_pages 1 \
  --taxonomy_path "/app/out/taxonomy_flextest_validated.jsonl" \
  --runner DirectRunner
```
*Output*:
* Directory `data_provenance/` with:
* One file `results.jsonl` which contains one line per species, with url links to the data sources used.

### 3. Push your image to your repo in GCP Artifacts repository

```bash
gcloud builds submit . --tag europe-west2-docker.pkg.dev/<projectid>/biodiversity-images/<image_name>:<your_tag> --project <projectid>
```

### 4. Create Dataflow Flex template

```bash
gcloud dataflow flex-template build gs://<my-bucket>/taxonomy_flex_template.json \
  --image europe-west2-docker.pkg.dev/<projectid>/biodiversity-images/<image_name>:<your_tag> \
  --sdk-language PYTHON \
  --metadata-file metadata_taxonomy.json \
  --project=<projectid> 
```

### 5. Run template

The Flex Template uses a custom Python package that must be available in Dataflow workers.
To ensure workers can import the pipeline code, the job must be run with a custom SDK container image.

When running the template (via gcloud, Airflow, or API), always pass:

```
--parameters sdk_container_image=europe-west2-docker.pkg.dev/<projectid>/biodiversity-images/<image_name>:<your_tag>
```
This image must be the same image used to build the Flex Template and must contain the installed pipeline package (e.g. dependencies).

Also, Runner v2 is required for custom SDK containers in batch pipelines. Also pass:

```bash
--parameters experiments=use_runner_v2
```
Flex Templates use one container to launch the job and a different container to run workers.
If `sdk_container_image` is not provided, Dataflow workers default to the Apache Beam SDK image, which does not contain this pipeline’s code and will fail with:

```bash 
ModuleNotFoundError: No module named 'dependencies'
```

Repeat the following command per pipeline:

```bash

gcloud dataflow flex-template run "taxonomy-$(date +%Y%m%d-%H%M%S)" \                                                                    
  --template-file-gcs-location "gs://<my-bucket>/taxonomy_flex_template.json" \
  --region=europe-west2 \
  --project=<projectid> \
  --parameters host=<host> \
  --parameters user=<user> \
  --parameters password=<password> \
  --parameters index="data_portal" \
  --parameters size=10 \
  --parameters pages=1 \
  --parameters output="gs://<my-bucket>/flex_taxonomy" \
  --parameters pipeline=taxonomy \
  --parameters runner=DataflowRunner
  --parameters sdk_container_image=europe-west2-docker.pkg.dev/<projectid>/biodiversity-images/<image_name>:<your_tag> \
  --parameters experiments=use_runner_v2
```
**Expected output**:
Same as above for each pipeline but in GSC and BiqQuery.

## Recommended object naming in Google Cloud Storage

Main GS bucket: `<your-bucket>`

Main “folder” name: `biodiv-pipelines-prod`

Dev name: `biodive-pipelines-dev`

### Pipelines "folder" structure: 

```bash

├── data
│   ├── bioregions
│   ├── climate
│   └── spatial_processing
├── out
│    ├── metadata
│    ├── occurrences_clean
│    ├── occurrences_raw
│    ├── spatial
│    ├── taxonomy
│
├── schemes
├── flex-templates
├── staging

```
### GSC object naming structure:

`<your-bucket>/biodiv-pipelines-prod/<any_above>`

**Example:** 

**In:** `<your-bucket>/biodiv-pipelines-prod/data/climate/<any_other_related_object>`

**Out:** `<your-bucket>/biodiv-pipelines-prod/out/occurrences_clean/occ_.*,jsonl`

### BigQuery Tables

**BigQuery dataset:** `gbdp`

**Tables:**

> **bp:** it stands for biodiveristy pipelines.

> **integ:** it stands for data integration in BigQuery via SQL statements. 

1. `bp_taxonomy_validated` → Taxonomic information from ENA and validated with GBIF species service.
2. `bp_gbif_occurrences` → Cleaned occurrence data. 
3. `bp_spatial_annotations` → Annotated occurrence data. 
4. `bp_species_range_estimates` → Range size estimates based on cleaned occurrence data. Convex hull/EOO. 
5. `bp_provenance_metadata` → URLs from data sources. 
6. `bp_summ_cleaning` → Summary of cleaning pipeline output. 
7. `bp_metadata_runs` → TODO: Table with logs for pipeline runs. 
8. `integ_genome_geatures` → Nested table containing all the tables above in Nested format by genome accession number. 
