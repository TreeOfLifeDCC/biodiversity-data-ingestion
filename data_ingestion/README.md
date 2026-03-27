# Biodiversity pipelines

Apache Beam batch pipelines for taxonomy validation, occurrence retrieval, cleaning, spatial annotation, range estimation, and provenance export.

## Quick start

### 1. Install dependencies

Use Python 3.12 and install the project dependencies:

Set your root directory at data_ingestion/.

```bash
pip install -r requirements.txt
```

### 2. Prerequisites

* Python 3.12
* Access to Elasticsearch (host, user, password, index)
* GCP project with Dataflow, BigQuery, and Cloud Storage access (Needed for runs in the Cloud)
* Local disk space for data inputs (`/data`) and intermediate outputs (`out/`) (Useful for local development)

### 3. Required data for spatial processing

Download and place the following datasets locally in `/data` (or use accessible paths).

From [Natural Earth](https://www.naturalearthdata.com/downloads/10m-physical-vectors/):

* Land polygons which you can download 
```bash
data/spatial_processing/ne_10m_land/ne_10m_land.shp
```
* Administrative centroids:
```bash
data/spatial_processing/ne_10m_admin_0_label_points/ne_10m_admin_0_label_points.shp
```
From [CHELSA](https://www.chelsa-climate.org/datasets/chelsa_bioclim)

* CHELSA-bioclim 1981-2010_V.2.1 rasters:

```bash
data/climate/CHELSA_bio*.tif
```
> Brun, P., Zimmermann, N. E., Hari, C., Pellissier, L., Karger, D. N. (2022). CHELSA-BIOCLIM+ A novel set of global climate-related predictors at kilometre-resolution. EnviDat. https://www.doi.org/10.16904/envidat.332.

From [UNEP-WCMC](https://data-gis.unep-wcmc.org/portal/home/item.html?id=0127920779a64e3f98925f2d3da3b847)

* Biogeographic regions (Ecoregions):

```bash
data/bioregions/Ecoregions2017.shp
```
> Dinerstein et al. An Ecoregion-Based Approach to Protecting Half the Terrestrial Realm, BioScience, Volume 67, Issue 6, June 2017, Pages 534–545, https://doi.org/10.1093/biosci/bix014

For convenience, you can download all the above files here: https://drive.google.com/drive/folders/1AQ-vggPieKmCjHA65cuvj7O1Q6nqDQab?usp=sharing

### 4. Project layout

Main pipeline entry points:

* `taxonomy_pipeline.py`
* `occurrences_pipeline.py`
* `cleaning_occs_pipeline.py`
* `spatial_annotation_pipeline.py`
* `range_estimation_pipeline.py`
* `data_provenance_pipeline.py`

### 3. Run pattern

Each pipeline follows the same local pattern:

```bash
python -m biodiv_pipelines.<pipeline> --arg1 value1 --arg2 value2
```

Beam runner options can also be passed after the pipeline arguments when needed.

## Pipeline order

Recommended execution order:

1. Taxonomy
2. Occurrences
3. Cleaning
4. Spatial annotation
5. Range estimation
6. Data provenance

## Minimal local execution (development)

Run pipelines locally using the DirectRunner (default).

Ensure all input data exists in GCS or in your local directories before running (schemas, climate, bioregions, etc.).

Define your variables:

```bash
export PROJECT_ID=<project-id>
export REGION=<your-region>
export BUCKET=gs://<your-bucket>
export BQ_DATASET=<your-bq-dataset>
```
### Taxonomy

```bash
python -m biodiv_pipelines.taxonomy_pipeline \
  --host <host> \
  --user <user> \
  --password <password> \
  --index <index> \
  --output "${BUCKET}/out/taxonomy/taxonomy" \
  --size 10 \
  --pages 1 \
  --sleep 0.25 \
  --bq_table "${PROJECT_ID}.${BQ_DATASET}.bq_taxonomy_validated" \
  --bq_schema "${BUCKET}/schemas/bq_taxonomy_schema.json" \
  --bq_gate_table "${PROJECT_ID}.${BQ_DATASET}.bp_log_taxonomy" \
  --temp_location "${BUCKET}/temp" \
  --project "${PROJECT_ID}" \
  --runner DirectRunner
```

**Supports:**
- Pagination control via `--size` and `--pages`
- Fetch-all mode (`--pages 0`)
- Incremental processing of new species only when `--bq_gate_table` is provided
- BigQuery output only when `--bq_table`. `--bq_schema` and `--temp_location` are provided.
- `--output` can be a local file or a GCS path

**Expected output:**
```text
out/taxonomy/
├── taxonomy_validated.jsonl
└── taxonomy_tocheck.jsonl
```
- `taxonomy_validated.jsonl`: species validated against GBIF and ready for the occurrences pipeline
- `taxonomy_tocheck.jsonl`: unresolved or lower-confidence matches that need manual review. Contains species with FUZZY, HIGHER RANK, and SYNONYMS matches.


### Occurrences

```bash
python -m biodiv_pipelines.occurrences_pipeline \
  --validated_input "${BUCKET}/out/taxonomy/taxonomy_validated.jsonl" \
  --output_dir "${BUCKET}/out/occurrences/raw" \
  --limit 10 \
  --runner DirectRunner
```
**NOTE**: using pygbif module occurrences.search.

**Supports:**
- `--limit` to limit the number of occurrences downloaded. Three hundred max.
- `--output_dir` to specify the output directory for raw occurrence records
- `--validated_input` and `--output_dir` support local files and GCS paths.

**Expected output:**
```text
out/occurrences/
├── occ_<species_name>.jsonl
├── summary_occ_download.jsonl
└── dead_records.jsonl   # only if failures occur
```

- `occ_<species_name>.jsonl`: one file per validated species with GBIF occurrence records
- `summary_occ_download.jsonl`: aggregate counts for succeeded species, failed species, and written occurrences
- `dead_records.jsonl`: failed GBIF retrievals, if any


### Cleaning

```bash
python -m biodiv_pipelines.cleaning_occs_pipeline \
  --input_glob "${BUCKET}/out/occurrences/occ_*.jsonl" \
  --output_dir "${BUCKET}/out/occurrences/clean" \
  --land_shapefile "${BUCKET}/data/spatial_processing/ne_10m_land/ne_10m_land.shp" \
  --centroid_shapefile "${BUCKET}/data/spatial_processing/ne_10m_admin_0_label_points/ne_10m_admin_0_label_points.shp" \
  --min_uncertainty 1000 \
  --max_uncertainty 5000 \
  --max_centroid_dist 5000 \
  --output_consolidated "${BUCKET}/out/pp/occurrences/clean/all_species" \
  --bq_table "${PROJECT_ID}.${BQ_DATASET}.bp_gbif_occurrences" \
  --bq_schema "${BUCKET}/schemas/bq_gbif_occurrences_schema.json" \
  --temp_location "${BUCKET}/temp" \
  --staging_location "${BUCKET}/staging" \
  --project "${PROJECT_ID}" \
  --runner DirectRunner
```
Filter raw occurrence records by removing invalid coordinates, centroids, points at the sea, and duplicates.

**Supports:**
- `land_shapefile` and `centroid_shapefile` are paths to shapefiles containing the land and centroids of the world.
- `min_uncertainty` and `max_uncertainty` are the maximum allowed uncertainty in meters for coordinates.
- `max_centroid_dist` is the maximum allowed distance in meters between a point and its centroid.
- `--output_consolidated` can be a local file or a GCS path.
- BigQuery output only when `--bq_table`, `--bq_schema`, and `--temp_location` are provided.
- All input and output files can be local or GCS paths.

**Expected output:**
```text
```text
out/occurrences/clean/
├── occ_<species_name>.jsonl
└── all_species.jsonl    # only if --output_consolidated is provided
```

- `occ_<species_name>.jsonl`: cleaned and deduplicated occurrences per species
- `all_species.jsonl`: consolidated cleaned occurrences across all species if `--output_consolidated` is provided.


### Spatial annotation

```bash
python -m biodiv_pipelines.spatial_annotation_pipeline \
  --input_occs "${BUCKET}/out/occurrences_clean/occ_*.jsonl" \
  --climate_dir "${BUCKET}/data/climate" \
  --biogeo_vector "${BUCKET}/data/bioregions/Ecoregions2017.shp" \
  --annotated_output "${BUCKET}/out/spatial/annotated" \
  --summary_output "${BUCKET}/out/spatial/summary" \
  --bq_summary_table "${PROJECT_ID}.${BQ_DATASET}.bp_spatial_annotations" \
  --bq_schema "${BUCKET}/schemas/bq_spatial_annotation_summ_schema.json" \
  --temp_location "${BUCKET}/temp" \
  --staging_location "${BUCKET}/staging" \
  --project "${PROJECT_ID}" \
  --runner DirectRunner
```
Use cleaned occurrence records to extract climate and area classification information from spatial data layers. At the moment: CHELSA climatologies and WWF Ecorregions (Dinnerstein et al. 2017).

**Supports:**
- `climate_dir` and `biogeo_vector` are paths to shapefiles containing the land and centroids of the world.
- `annotated_output` and `summary_output` are paths to output directories.
- BigQuery output only when `--bq_summary_table`, `--bq_schema`, and `--temp_location` are provided.
- All input and output files can be local or GCS paths.

**Expected outpur:**
```text
out/spatial/
├── annotated.jsonl
└── summary.jsonl
```

- `annotated.jsonl`: occurrence-level climate and biogeographic annotations
- `summary.jsonl`: accession-level summary statistics for climate and biogeographic regions


### Range estimation

This pipeline uses the `range_km2` field in the occurrence records to estimate the geographic range of each species.

It does not produce a text file output like the other pipelines. Instead, it writes the results to a BigQuery table.

```bash
python range_estimation_pipeline.py \
  --input_glob "${BUCKET}/out/occurrences/clean/occ_*.jsonl" \
  --bq_table "${PROJECT_ID}.${BQ_DATASET}.bp_species_range_estimates" \
  --bq_schema "${BUCKET}/schemas/bq_range_estimates_schema.json" \
  --temp_location "${BUCKET}/temp" \
  --staging_location "${BUCKET}/staging" \
  --project "${PROJECT_ID}" \
  --runner DirectRunner
```
**Expected output:**
- One row per species with estimated convex hull range area in km²
- If fewer than 3 valid coordinates are available, range_km2 is null and a note is added


### Data provenance

```bash
python -m biodiv_pipelines.data_provenance_pipeline \
  --taxonomy_path "${BUCKET}/out/taxonomy_validated.jsonl" \
  --host <host> \
  --user <user> \
  --password <password> \
  --index <index> \
  --min_batch_size 50 \
  --max_batch_size 200 \
  --taxonomy_path "${BUCKET}/out/taxonomy/taxonomy_validated.jsonl" \
  --output "${BUCKET}/out/metadata/provenance_urls_new.jsonl" \
  --bq_table "${PROJECT_ID}.${BQ_DATASET}.bp_provenance_metadata" \
  --bq_schema "${BUCKET}/schemas/bq_metadata_url_schema.json" \
  --temp_location "${BUCKET}/temp" \
  --staging_location "${BUCKET}/staging" \
  --project "${PROJECT_ID}" \
  --runner DirectRunner
```
**Supports:**
- `min_batch_size` and `max_batch_size` control the number of species per batch in Beam. Adjust only if there are performance issues.
- BigQuery output only when `--bq_table`, `--bq_schema`, and `--temp_location` are provided.
- All input and output files can be local or GCS paths.

**Expected output:**
```text
out/metadata/
└── provenance_urls_new.jsonl
```
- One row per species with source URLs and identifiers, including Biodiversity Portal, GTF, Ensembl browser, and GBIF links.


## Direct Run on Google Cloud Dataflow

Use Dataflow after validating the pipelines locally.

### 1. Docker image

#### Define variables:

```bash
export PROJECT_ID=<project-id>
export REGION=<your-region>
export BUCKET=gs://<your-bucket>
export TAG=<your-tag>
export IMAGE=${REGION}-docker.pkg.dev/${PROJECT_ID}/biodiversity-images/biodiv-pipeline:${TAG}
```

#### Build and push the Docker image

Set your root directory at data_ingestion/ and execute the following commands:

Using Docker:

```bash
docker build --no-cache -f Dockerfile -t ${IMAGE} .
docker push ${IMAGE}
```
Or directly on Google Cloud using the gcloud library:

```bash
gcloud builds submit . \
  --tag ${IMAGE} \
  --project ${PROJECT_ID}
```

### 2. Example: Taxonomy pipeline on Dataflow

```bash
python -m biodiv_pipelines.taxonomy_pipeline \
  --host <host> \
  --user <user> \
  --password <password> \
  --index <index> \
  --size 10 \
  --pages 1 \
  --sleep 0.25 \
  --output "${BUCKET}/out/taxonomy/taxonomy" \
  --bq_table "${PROJECT_ID}.${BQ_DATASET}.bq_taxonomy_validated" \
  --bq_schema "${BUCKET}/schemas/bq_taxonomy_schema.json" \
  --bq_gate_table "${PROJECT_ID}.${BQ_DATASET}.bp_log_taxonomy" \
  --runner DataflowRunner \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --temp_location "${BUCKET}/temp" \
  --staging_location "${BUCKET}/staging" \
  --sdk_container_image "${IMAGE}" \
  --save_main_session
```

### 3. Pattern for other pipelines

To run any other pipeline on Dataflow:

1. Start from the local command
2. Replace:
local paths → `gs://` paths & `--runner DirectRunner `→ `--runner DataflowRunner`
3. Ensure the following flags are always present:

```bash
--runner DataflowRunner \
--project "${PROJECT_ID}" \
--region "${REGION}" \
--temp_location "${BUCKET}/temp" \
--staging_location "${BUCKET}/staging" \
--sdk_container_image "${IMAGE}" \
--save_main_session
```

## Run using Dataflow Flex Templates

For production and orchestration (e.g. Cloud Composer), pipelines should be executed using Dataflow Flex Templates instead of direct `python -m` submission.

Flex Templates package:
- the pipeline code
- dependencies (via Docker image)
- runtime parameters

The entry point is `main.py`. This calls `launcher.py`, which contains the `launch_pipeline` function that runs the pipeline via the `--pipeline` command line.

## How to run this repo

```bash
git clone git@github.com:TreeOfLifeDCC/biodiversity-data-ingestion.git
```

Set your root directory at data_ingestion/.

### 1. Build docker image

To test this image locally, you will need to include the data directory. This contains all the raster and vector layers required for the spatial cleaning and annotation pipelines.
Download it from here: https://drive.google.com/drive/folders/1AQ-vggPieKmCjHA65cuvj7O1Q6nqDQab?usp=sharing

**Define variables:**

```bash
export PROJECT_ID=<project-id>
export REGION=<your-region>
export BUCKET=gs://<your-bucket>
export TAG=<your-tag>
export IMAGE=${REGION}-docker.pkg.dev/${PROJECT_ID}/biodiversity-images/biodiv-pipeline:${TAG}
```

```bash
docker build -f Dockerfile -t europe-west2-docker.pkg.dev/${PROJECT_ID}/biodiversity-images/${IMAGE} .
```
### 2. Test the docker image locally

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

**NOTE**: Follow the same pattern for other pipelines.


### 3. Push and build your image into the GCP Artifacts repository

```bash
gcloud builds submit . --tag ${REGION}-docker.pkg.dev/${PROJECT_ID}/biodiversity-images/${IMAGE} --project ${PROJECT_ID}
```

### 4. Create Dataflow Flex template

```bash
gcloud dataflow flex-template build gs://${BUCKET}/taxonomy_flex_template.json \
  --image ${REGION}-docker.pkg.dev/${PROJECT_ID}/biodiversity-images/${IMAGE} \
  --sdk-language PYTHON \
  --metadata-file metadata_taxonomy.json \
  --project=${PROJECT_ID} 
```
**NOTE**: The metadata file is used to specify the parameters that will be passed to the pipeline. This file must be available in your local directory from where you run the command.

### 5. Run template

The Flex Template uses a custom Python package that must be available in Dataflow workers.
To ensure workers can import the pipeline code, the job must be run with a custom SDK container image.

When running the template (via gcloud, Airflow, or API), always pass:

```
--parameters sdk_container_image=${REGION}-docker.pkg.dev/${PROJECT_ID}/biodiversity-images/${IMAGE}
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

Repeat the following command per pipeline using the respective parameters:

```bash

gcloud dataflow flex-template run "taxonomy-$(date +%Y%m%d-%H%M%S)" \                                                                    
  --template-file-gcs-location "gs://${BUCKET}/taxonomy_flex_template.json" \
  --region=europe-west2 \
  --project=${PROJECT_ID} \
  --parameters host=<host> \
  --parameters user=<user> \
  --parameters password=<password> \
  --parameters index="data_portal" \
  --parameters size=10 \
  --parameters pages=1 \
  --parameters output="gs://${BUCKET}/flex_taxonomy" \
  --parameters pipeline=taxonomy \
  --parameters runner=DataflowRunner
  --parameters sdk_container_image=${REGION}-docker.pkg.dev/${PROJECT_ID}/biodiversity-images/${IMAGE} \
  --parameters experiments=use_runner_v2
```
**Expected output**:
Same as above for each pipeline but in GCS and BiqQuery.

## Recommended object naming in Google Cloud Storage

Main GS bucket: `<your-bucket>`

Main “folder” name: `biodiv-pipelines-prod`

Dev name: `biodive-pipelines-dev`

### Pipelines "folder" structure: 

```bash
biodiv-pipelines-dev/
├── data/
│   ├── bioregions/
│   ├── climate/
│   └── spatial_processing/
├── out
│    ├── metadata/
│    ├── occurrences/
│    ├── spatial/
│    └── taxonomy/
│
├── schemes
├── flex-templates
├── staging
└── temp

```
### GSC object naming structure:

`<your-bucket>/biodiv-pipelines-prod/<any_above>`

**Example:** 

**In:** `<your-bucket>/biodiv-pipelines-prod/data/climate/<any_other_related_object>`

**Out:** `<your-bucket>/biodiv-pipelines-prod/out/occurrences_clean/occ_.*,jsonl`

### BigQuery Tables

**BigQuery prod dataset:** `gbdp`
**BigQuery dev dataset:** `your_dev_dataset`

**Tables:**

> **bp:** it stands for biodiveristy pipelines.

> **integ:** it stands for data integration in BigQuery via SQL statements. 

1. `bp_taxonomy_validated` → Taxonomic information from ENA and validated with GBIF species service.
2. `bp_gbif_occurrences` → Cleaned occurrence data. 
3. `bp_spatial_annotations` → Annotated occurrence data. 
4. `bp_species_range_estimates` → Range size estimates based on cleaned occurrence data. Convex hull/EOO. 
5. `bp_provenance_metadata` → URLs from data sources. 
6. `bp_summ_cleaning` → Summary of cleaning pipeline output. 
7. `bp_log_taxonomy` → Gate table to decide incremental runs. Logs for all the species that have been processed through the pipeline.
8. `integ_genome_geatures` → Nested table containing all the tables above in Nested format by genome accession number. 
