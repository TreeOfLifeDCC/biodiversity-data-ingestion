# Dataflow Flex Templates for the Biodiversity pipelines

The entry point is `main.py`. This calls `launcher.py`, which contains the `launch_pipeline` function that runs the pipeline via the `--pipeline` command line.
Currently, only the taxonomy pipeline (`--pipeline=taxonomy_pipeline.py`) has been wrapped in a flex template. (Note: the BigQuery steps in the pipeline have not yet been tested with the Flex template.)

## How to run this repo

```bash
git clone git@github.com:JuanPNG/dev_biodiv_flex.git
```

Replace `<xxxx>` with the actual value.

1. Build docker image
```bash
docker build -f Dockerfile -t europe-west2-docker.pkg.dev/<projectid>/biodiversity-images/biodiversity-flex2:latest .
```
2. Test locally
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
  --output /app/out/your_file_name \
  --size 10 \
  --pages 1 \
  --sleep 0.25 \
  --direct_num_workers=1
```
3. Push your image to your repo in GCP Artifacts repository
```bash
 docker push europe-west2-docker.pkg.dev/<projectid>/biodiversity-images/biodiversity-flex:<your_tag>
```

4. Create Dataflow Flex template
```bash
gcloud dataflow flex-template build gs://<my-bucket>/taxonomy_flex_template.json \
  --image europe-west2-docker.pkg.dev/<projectid>/biodiversity-images/biodiversity-flex2:<your_tag> \
  --sdk-language PYTHON \
  --metadata-file metadata_taxonomy.json \
  --project=<projectid> 
```

5. Run template
```bash

gcloud dataflow flex-template run "taxonomy-$(date +%Y%m%d-%H%M%S)" \                                                                    
  --template-file-gcs-location "gs://<my-bucket>/test_jpng/flex-templates/taxonomy_template.json" \
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
```
**Expected output**:
* `flex_taxonomy_tocheck.jsonl` (0 rows)
* `flex_taxonomy_validated.jsonl` (10 rows)