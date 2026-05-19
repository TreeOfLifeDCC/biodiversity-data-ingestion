def transform_to_aegis_format(tax_id_and_samples, annotations=None):

    tax_id, samples_iterable = tax_id_and_samples
    samples = list(samples_iterable)

    if not samples:
        return None

    # use the first sample to extract species-level information, all samples with same tax_id have same species
    first_sample = samples[0]

    aegis_record = {}

    aegis_record["taxId"] = tax_id
    aegis_record["scientificName"] = extract_scientific_name(first_sample)
    aegis_record["commonName"] = extract_common_name(first_sample)

    aegis_record["phylogeny"] = extract_phylogeny(first_sample)

    aegis_record["samples"] = transform_samples(samples)

    # aggregate experiments and assemblies from all samples
    all_experiments = []
    all_assemblies = []
    for sample in samples:
        if "experiments" in sample:
            all_experiments.extend(sample["experiments"])
        if "assemblies" in sample:
            all_assemblies.extend(sample["assemblies"])

    aegis_record["rawData"] = all_experiments
    aegis_record["assemblies"] = enrich_assemblies(all_assemblies, annotations)

    # status fields
    aegis_record["bioSamplesStatus"] = "Done"  # if we have samples, biosamples is done
    aegis_record["rawDataStatus"] = "Done" if all_experiments else "Waiting"
    aegis_record["assembliesStatus"] = "Done" if all_assemblies else "Waiting"

    # current status --> highest level achieved
    aegis_record["currentStatus"] = determine_current_status(
        has_assemblies=bool(all_assemblies),
        has_experiments=bool(all_experiments)
    )

    aegis_record["currentStatusOrder"] = calculate_status_order(
        aegis_record["currentStatus"]
    )

    return aegis_record


def extract_scientific_name(sample):
    if "characteristics" in sample:
        if "organism" in sample["characteristics"]:
            organism_list = sample["characteristics"]["organism"]
            if organism_list and len(organism_list) > 0:
                return organism_list[0].get("text", "Unknown")

    return sample.get("name", "Unknown")


def extract_common_name(sample):
    if "characteristics" in sample:
        if "common name" in sample["characteristics"]:
            common_name_list = sample["characteristics"]["common name"]
            if common_name_list and len(common_name_list) > 0:
                return common_name_list[0].get("text", "Not specified")

    return "Not specified"


def extract_phylogeny(sample):
    return sample.get("taxonomy", {})


def transform_samples(samples):
    transformed_samples = []

    for sample in samples:
        chars = sample.get("characteristics", {})

        def get_char(key):
            val = chars.get(key, [])
            return val[0].get("text", "") if val else ""

        sample_record = {
            "accession": sample.get("accession", ""),
            "scientificName": extract_scientific_name(sample),
            "commonName": get_char("common name"),
            "habitat": get_char("habitat"),
            "lifestage": get_char("lifestage"),
            "sex": get_char("sex"),
            "organismPart": get_char("organism part"),
            "lat": get_char("geographic location (latitude)"),
            "lon": get_char("geographic location (longitude)"),
            "country": get_char("geographic location (country and/or sea)"),
            "locality": get_char("geographic location (region and locality)"),
            "tolid": get_char("tolid"),
            "trackingSystem": sample.get("project_name", ""),
        }

        transformed_samples.append(sample_record)

    return transformed_samples


def determine_current_status(has_assemblies, has_experiments):
    if has_assemblies:
        return "Assemblies - Submitted"
    elif has_experiments:
        return "Raw Data - Submitted"
    else:
        return "Submitted to BioSamples"


def calculate_status_order(current_status):
    status_map = {
        "Assemblies - Submitted": 3,
        "Raw Data - Submitted": 2,
        "Submitted to BioSamples": 1
    }

    return status_map.get(current_status, 1)


def enrich_assemblies(assemblies, annotations):
    """Attach genebuild annotation status and metrics to each assembly.

    `annotations` is a dict keyed by gca_accession (e.g. "GCA_964606135.1"),
    supplied by the pipeline's annotation side input. The join key for each
    assembly is built as f"{accession}.{version}" to match that form.

    When `annotations` is None or empty (no annotation file was provided to the
    pipeline), assemblies are returned unchanged, so the output is identical to
    the pre-annotation pipeline. Assemblies with no matching annotation are also
    left untouched.
    """
    if not annotations:
        return assemblies

    enriched = []
    for assembly in assemblies:
        accession = assembly.get("accession", "")
        version = assembly.get("version", "")
        key = f"{accession}.{version}" if accession and version else accession

        annotation = annotations.get(key)
        if annotation:
            # build a new dict rather than mutating the grouped input value
            assembly = {
                **assembly,
                "annotation": {
                    "status": annotation.get("gb_status"),
                    "method": annotation.get("annotation_method"),
                    "source": annotation.get("annotation_source"),
                    "dateStarted": annotation.get("date_started"),
                    "dateStatusUpdate": annotation.get("date_status_update"),
                    "lastGenebuildUpdate": annotation.get("last_genebuild_update"),
                    "releaseDate": annotation.get("release_date"),
                    "releaseType": annotation.get("release_type"),
                    "metrics": annotation.get("metrics", {}),
                },
            }

        enriched.append(assembly)

    return enriched