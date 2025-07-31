FROM python:3.12-slim

# Set locale for I/O and encoding
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Copy SDK entrypoint binary from Apache Beam image, which makes it possible to
# use the image as SDK container image. If you explicitly depend on
# apache-beam in setup.py, use the same version of Beam in both files.
COPY --from=apache/beam_python3.12_sdk:2.62.0 /opt/apache/beam /opt/apache/beam

# Copy Flex Template launcher binary from the launcher image, which makes it
# possible to use the image as a Flex Template base image.
COPY --from=gcr.io/dataflow-templates-base/python312-template-launcher-base:latest /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

# Location to store the pipeline artifacts.
ARG WORKDIR=/template
WORKDIR ${WORKDIR}

COPY data_ingestion/main.py .
COPY data_ingestion/pyproject.toml .
COPY data_ingestion/requirements.txt .
COPY data_ingestion/setup.py .
COPY data_ingestion/src src

# Install geo/raster system spatial libraries to meet requirements of geospatial analysis modules in Python
RUN apt-get update && apt-get install -y \
    build-essential \
    gdal-bin \
    libgdal-dev \
    libspatialindex-dev \
    libsnappy1v5 \
    locales \
    && rm -rf /var/lib/apt/lists/*

# Installing exhaustive list of dependencies from a requirements.txt
# helps to ensure that every time Docker container image is built,
# the Python dependencies stay the same. Using `--no-cache-dir` reduces image size.
RUN pip install --no-cache-dir -r requirements.txt

# Installing the pipeline package makes all modules encompassing the pipeline
# available via import statements and installs necessary dependencies.
# Editable installation allows picking up later changes to the pipeline code
# for example during local experimentation within the container.
RUN pip install -e .

# For more informaiton, see: https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"

# Because this image will be used as custom sdk container image, and it already
# installs the dependencies from the requirements.txt, we can omit
# the FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE directive here
# to reduce pipeline submission time.
# Similarly, since we already installed the pipeline package,
# we don't have to specify the FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py" configuration option.

# Optionally, verify that dependencies are not conflicting.
# A conflict may or may not be significant for your pipeline.
RUN pip check

# Optionally, list all installed dependencies.
# The output can be used to seed requirements.txt for reproducible builds.
RUN pip freeze

# Set the entrypoint to Apache Beam SDK launcher, which allows this image
# to be used as an SDK container image.
ENTRYPOINT ["/opt/apache/beam/boot"]
