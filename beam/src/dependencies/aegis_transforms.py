def transform_to_aegis_format(tax_id_and_samples):

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
    aegis_record["assemblies"] = all_assemblies

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
    phylogeny = {}

    phylogenetic_ranks = [
        'kingdom', 'phylum', 'class', 'order',
        'family', 'genus', 'species'
    ]

    if "characteristics" not in sample:
        return phylogeny

    characteristics = sample["characteristics"]

    for rank in phylogenetic_ranks:
        if rank in characteristics:
            rank_list = characteristics[rank]
            if rank_list and len(rank_list) > 0:
                phylogeny[rank] = rank_list[0].get("text", "")

    return phylogeny


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