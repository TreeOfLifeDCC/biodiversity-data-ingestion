"""
Defines map functions to be used within my_pipeline
"""

import requests
import apache_beam as beam

from lxml import etree

from dependencies.utils.common_functions import (
    check_field_existence,
    get_common_name,
    parse_data_records,
    parse_data_records_dwh,
    remove_duplicated_metadata_records,
    remove_duplicated_data_records,
)

SPECIMENS_SYMBIONTS_CHECKLISTS = ["ERC000011", "ERC000053"]
METAGENOMES_CHECKLISTS = [
    "ERC000013",
    "ERC000024",
    "ERC000025",
    "ERC000047",
    "ERC000050",
]


# TODO: add type hints
def classify_samples(sample):
    """
    Classify incoming samples into specimens, symbionts or metagenomes
    Args:
        sample (dict[str, str|dict|list]): sample to classify
    Returns:
        sample (dict[str, str|dict|list]): classified sample
    """
    error_sample = dict()
    try:
        error_sample["biosample_id"] = sample["accession"]
    except KeyError:
        return beam.pvalue.TaggedOutput("Errors", sample)
    try:
        checklist = sample["characteristics"]["ENA-CHECKLIST"][0]["text"]
        if checklist in SPECIMENS_SYMBIONTS_CHECKLISTS:
            if (
                "symbiont" in sample["characteristics"]
                and sample["characteristics"]["symbiont"][0]["text"] == "Y"
            ):
                return beam.pvalue.TaggedOutput("Symbionts", sample)
            else:
                return sample
        elif checklist in METAGENOMES_CHECKLISTS:
            return beam.pvalue.TaggedOutput("Metagenomes", sample)
        else:
            return sample
    except KeyError:
        return sample


# TODO: add type hints
def process_specimens_for_elasticsearch(sample):
    """
    Parses incoming samples into Elasticsearch compatible record
    Args:
        sample (dict[str, str|dict|list]): sample to process
    Returns:
        sample (dict[str, str|dict|list]): processed sample, ready for Elasticsearch
    """
    data_portal_record = dict()
    data_portal_record["accession"] = sample["accession"]

    # Adding data status to record based on available data
    if "experiments" in sample and len(sample["experiments"]) > 0:
        data_portal_record["tracking_status"] = "Raw Data - Submitted"
    elif "assemblies" in sample and len(sample["assemblies"]) > 0:
        data_portal_record["tracking_status"] = "Assemblies - Submitted"
    else:
        data_portal_record["tracking_status"] = "Submitted to BioSamples"
    data_portal_record["customFields"] = list()

    # Parsing all other fields excluding organism as it goes to the root
    for record_name, record in sample["characteristics"].items():
        if record_name != "organism":
            values, units, ontology_terms = check_field_existence(record)
            data_portal_record["customFields"].append(
                {
                    "name": record_name,
                    "value": values,
                    "unit": units,
                    "ontology_term": ontology_terms,
                }
            )
            if "NHMUK" in values:
                try:
                    images_response = requests.get(
                        f"https://portal.erga-biodiversity.eu/api/images/{values}"
                    ).json()
                    if len(images_response["results"]) > 0:
                        data_portal_record["images"] = images_response["results"][0][
                            "_source"
                        ]["images"]
                except KeyError:
                    pass

    # Adding relationships
    if "relationships" in sample and len(sample["relationships"]) > 0:
        data_portal_record["relationships"] = sample["relationships"]

    # Adding taxId
    try:
        data_portal_record["taxId"] = sample["taxId"]
    except KeyError:
        data_portal_record["taxId"] = None

    # Adding organism
    try:
        organism_name, organism_ontology, _ = check_field_existence(
            sample["characteristics"]["organism"]
        )
        data_portal_record["organism"] = {
            "text": organism_name,
            "ontologyTerm": organism_ontology,
        }
    except KeyError:
        data_portal_record["organism"] = {"text": None, "ontologyTerm": None}

    return data_portal_record


def process_samples_for_dwh(sample, sample_type):
    error_sample = dict()
    error_sample["biosample_id"] = sample["accession"]

    dwh_record = dict()
    dwh_record["accession"] = sample["accession"]

    try:
        organism_name, organism_ontology, _ = check_field_existence(
            sample["characteristics"]["organism"]
        )
        dwh_record["organism"] = {
            "text": organism_name,
            "ontologyTerm": organism_ontology,
        }
    except KeyError:
        dwh_record["organism"] = {"text": None, "ontologyTerm": None}

    if dwh_record["organism"]["text"]:
        dwh_record["commonName"] = get_common_name(dwh_record["organism"]["text"])
    else:
        dwh_record["commonName"] = None

    try:
        sex_name, _, _ = check_field_existence(sample["characteristics"]["sex"])
        dwh_record["sex"] = sex_name
    except KeyError:
        dwh_record["sex"] = None

    try:
        organism_part_name, _, _ = check_field_existence(
            sample["characteristics"]["organism part"]
        )
        dwh_record["organismPart"] = organism_part_name
    except KeyError:
        dwh_record["organismPart"] = None

    try:
        tolid, _, _ = check_field_existence(sample["characteristics"]["tolid"])
        dwh_record["tolid"] = tolid
    except KeyError:
        dwh_record["tolid"] = None

    try:
        lat, _, _ = check_field_existence(
            sample["characteristics"]["geographic location (latitude)"]
        )
        dwh_record["lat"] = lat
    except KeyError:
        dwh_record["lat"] = None

    try:
        lon, _, _ = check_field_existence(
            sample["characteristics"]["geographic location (longitude)"]
        )
        dwh_record["lon"] = lon
    except KeyError:
        dwh_record["lon"] = None

    try:
        locality, _, _ = check_field_existence(
            sample["characteristics"]["geographic location (region and locality)"]
        )
        dwh_record["locality"] = locality
    except KeyError:
        dwh_record["locality"] = None

    try:
        country, _, _ = check_field_existence(
            sample["characteristics"]["geographic location (country and/or sea)"]
        )
        dwh_record["country"] = country
    except KeyError:
        dwh_record["country"] = None

    if "experiments" in sample and len(sample["experiments"]) > 0:
        dwh_record["trackingSystem"] = "Raw Data - Submitted"
    elif "assemblies" in sample and len(sample["assemblies"]) > 0:
        dwh_record["trackingSystem"] = "Assemblies - Submitted"
    else:
        dwh_record["trackingSystem"] = "Submitted to BioSamples"

    try:
        dwh_record["experiments"] = sample["experiments"]
    except KeyError:
        dwh_record["experiments"] = list()
    try:
        dwh_record["assemblies"] = sample["assemblies"]
    except KeyError:
        dwh_record["assemblies"] = list()
    try:
        dwh_record["analyses"] = sample["analyses"]
    except KeyError:
        dwh_record["analyses"] = list()

    try:
        dwh_record["lifestage"], _, _ = check_field_existence(
            sample["characteristics"]["lifestage"]
        )
    except KeyError:
        dwh_record["lifestage"] = None

    try:
        dwh_record["habitat"], _, _ = check_field_existence(
            sample["characteristics"]["habitat"]
        )
    except KeyError:
        dwh_record["habitat"] = None

    dwh_record["project_name"] = sample["project_name"]

    if dwh_record["organism"]["text"] == "Ochlodes sylvanus":
        return "876063_3126489", dwh_record

    if sample_type == "specimens":
        return sample["taxId"], dwh_record
    elif sample_type == "symbionts":
        try:
            host_biosample_id = sample["characteristics"]["sample symbiont of"][0][
                "text"
            ]
        except KeyError:
            error_sample["error_message"] = (
                "missing 'sample symbiont of' field for symbiont sample"
            )
            return beam.pvalue.TaggedOutput("Errors", error_sample)
        host_sample = requests.get(
            f"https://www.ebi.ac.uk/biosamples/samples/{host_biosample_id}.json"
        ).json()
        if host_sample["taxId"] == 3126489:
            return "876063_3126489", dwh_record
        return host_sample["taxId"], dwh_record
    else:
        try:
            host_biosample_id = sample["characteristics"]["sample derived from"][0][
                "text"
            ]
        except KeyError:
            error_sample["error_message"] = (
                "missing 'sample derived from' field for metagenome sample"
            )
            return beam.pvalue.TaggedOutput("Errors", error_sample)
        host_sample = requests.get(
            f"https://www.ebi.ac.uk/biosamples/samples/{host_biosample_id}.json"
        ).json()
        if "characteristics" not in host_sample:
            error_sample["error_message"] = "Host sample doesn't exist"
            return beam.pvalue.TaggedOutput("Errors", error_sample)
        while host_sample["characteristics"]["ENA-CHECKLIST"][0]["text"] != "ERC000053":
            try:
                host_biosample_id = host_sample["characteristics"][
                    "sample derived from"
                ][0]["text"]
            except KeyError:
                error_sample["error_message"] = (
                    "missing 'sample derived from' field for metagenome sample"
                )
                return beam.pvalue.TaggedOutput("Errors", error_sample)
            host_sample = requests.get(
                f"https://www.ebi.ac.uk/biosamples/samples/{host_biosample_id}.json"
            ).json()
        return host_sample["taxId"], dwh_record


def build_data_portal_record(element, bq_dataset_name, genome_notes, annotations):
    # TODO: split this function
    phylogenetic_ranks = (
        "kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "species",
        "cohort",
        "forma",
        "infraclass",
        "infraorder",
        "parvorder",
        "section",
        "series",
        "species_group",
        "species_subgroup",
        "subclass",
        "subcohort",
        "subfamily",
        "subgenus",
        "subkingdom",
        "suborder",
        "subphylum",
        "subsection",
        "subspecies",
        "subtribe",
        "superclass",
        "superfamily",
        "superkingdom",
        "superorder",
        "superphylum",
        "tribe",
        "varietas",
    )
    sample = dict()

    # host metadata
    sample["tax_id"] = element[0]
    sample["currentStatus"] = "Submitted to BioSamples"

    (
        sample["experiment"],
        sample["assemblies"],
        sample["analyses"],
        sample["project_name"],
    ) = parse_data_records(element[1]["specimens"])
    sample["records"] = [
        {
            k: v
            for k, v in specimens.items()
            if k not in ["experiments", "assemblies", "analyses", "project_name"]
        }
        for specimens in element[1]["specimens"]
    ]

    # Nagoya protocol links
    show_nagoya_protocol = False
    for record in sample["records"]:
        if record["country"] is not None and "Spain" in record["country"]:
            record["nagoya_protocol"] = True
            show_nagoya_protocol = True
    if show_nagoya_protocol:
        sample["nagoya_protocol"] = True

    # Taxonomies
    sample["taxonomies"] = dict()
    if sample["tax_id"] == "876063_3126489":
        response = requests.get("https://www.ebi.ac.uk/ena/browser/api/xml/3126489")
    else:
        response = requests.get(
            f"https://www.ebi.ac.uk/ena/browser/api/xml/{sample['tax_id']}"
        )
    root = etree.fromstring(response.content)
    try:
        sample["organism"] = root.find("taxon").get("scientificName")
    except AttributeError:
        error_sample = {"tax_id": None, "scientific_name": None, "common_name": None}
        return beam.pvalue.TaggedOutput("error", error_sample)

    try:
        sample["commonName"] = root.find("taxon").get("commonName")
    except AttributeError:
        sample["commonName"] = element[1]["specimens"][0]["commonName"]
    if sample["commonName"] is None:
        sample["commonName"] = element[1]["specimens"][0]["commonName"]
    sample["commonNameSource"] = "NCBI_taxon"

    # adding phylogenetic information
    for rank in phylogenetic_ranks:
        sample["taxonomies"][rank] = {
            "scientificName": "Other",
            "commonName": "Other",
            "tax_id": None,
        }

    try:
        for taxon in root.find("taxon").find("lineage").findall("taxon"):
            rank = taxon.get("rank")
            if rank in phylogenetic_ranks:
                scientific_name = taxon.get("scientificName")
                common_name = taxon.get("commonName")
                tax_id = taxon.get("taxId")
                sample["taxonomies"][rank]["scientificName"] = (
                    scientific_name if scientific_name else "Other"
                )
                sample["taxonomies"][rank]["commonName"] = (
                    common_name if common_name else "Other"
                )
                sample["taxonomies"][rank]["tax_id"] = tax_id if tax_id else None
    except AttributeError:
        error_sample = {
            "tax_id": sample["tax_id"],
            "scientific_name": sample["organism"],
            "common_name": sample["commonName"],
        }
        return beam.pvalue.TaggedOutput("error", error_sample)

    if sample["tax_id"] in [624, 1773, 2697049]:
        error_sample = {"tax_id": None, "scientific_name": None, "common_name": None}
        return beam.pvalue.TaggedOutput("error", error_sample)

    # symbionts and metagenomes raw data
    (
        sample["symbionts_experiment"],
        sample["symbionts_assemblies"],
        sample["symbionts_analyses"],
        _,
    ) = parse_data_records(element[1]["symbionts"])
    sample["symbionts_records"] = [
        {
            k: v
            for k, v in specimens.items()
            if k not in ["experiments", "assemblies", "analyses", "project_name"]
        }
        for specimens in element[1]["symbionts"]
    ]
    (
        sample["metagenomes_experiment"],
        sample["metagenomes_assemblies"],
        sample["metagenomes_analyses"],
        _,
    ) = parse_data_records(element[1]["metagenomes"])
    sample["metagenomes_records"] = [
        {
            k: v
            for k, v in specimens.items()
            if k not in ["experiments", "assemblies", "analyses", "project_name"]
        }
        for specimens in element[1]["metagenomes"]
    ]

    if len(sample["symbionts_records"]) > 0:
        sample["symbionts_biosamples_status"] = "Submitted to BioSamples"
    if len(sample["symbionts_assemblies"]) > 0:
        sample["symbionts_assemblies_status"] = "Assemblies Submitted"

    if len(sample["metagenomes_records"]) > 0:
        sample["metagenomes_biosamples_status"] = "Submitted to BioSamples"
    if len(sample["metagenomes_assemblies"]) > 0:
        sample["metagenomes_assemblies_status"] = "Assemblies Submitted"

    # host data status
    sample["currentStatus"] = (
        "Raw Data - Submitted"
        if len(sample["experiment"]) != 0
        else sample["currentStatus"]
    )
    sample["currentStatus"] = (
        "Assemblies - Submitted"
        if len(sample["assemblies"]) != 0
        else sample["currentStatus"]
    )

    # request annotations
    annotations_dict = {
        str(annotation["tax_id"]): annotation["annotations"]
        for annotation in annotations
    }
    if sample["tax_id"] == "876063_3126489":
        sample["currentStatus"] = "Annotation Complete"
        sample["annotation"] = annotations_dict["3126489"]
    elif str(sample["tax_id"]) in annotations_dict:
        sample["currentStatus"] = "Annotation Complete"
        sample["annotation"] = annotations_dict[str(sample["tax_id"])]

    sample["biosamples"] = "Done"
    sample["annotation_status"] = "Waiting"
    if sample["currentStatus"] == "Annotation Complete":
        sample["annotation_complete"] = "Done"
    else:
        sample["annotation_complete"] = "Waiting"
    if len(sample["assemblies"]) > 0:
        sample["assemblies_status"] = "Done"
    else:
        sample["assemblies_status"] = "Waiting"
    if len(sample["experiment"]) > 0:
        sample["mapped_reads"] = "Done"
        sample["raw_data"] = "Done"
    else:
        sample["mapped_reads"] = "Waiting"
        sample["raw_data"] = "Waiting"
    sample["trackingSystem"] = [
        {"name": "biosamples", "status": sample["biosamples"], "rank": 1},
        {"name": "mapped_reads", "status": sample["mapped_reads"], "rank": 2},
        {"name": "assemblies", "status": sample["assemblies_status"], "rank": 3},
        {"name": "raw_data", "status": sample["raw_data"], "rank": 4},
        {"name": "annotation", "status": sample["annotation_status"], "rank": 5},
        {
            "name": "annotation_complete",
            "status": sample["annotation_complete"],
            "rank": 6,
        },
    ]

    # Collecting tolids and coordinates
    sample["tolid"] = set()
    sample["orgGeoList"] = list()
    sample["specGeoList"] = list()
    for specimen in element[1]["specimens"]:
        if specimen["tolid"] is not None:
            sample["tolid"].add(specimen["tolid"])
        tmp = dict()
        tmp["organism"] = specimen["organism"]["text"]
        tmp["accession"] = specimen["accession"]
        tmp["commonName"] = specimen["commonName"]
        tmp["sex"] = specimen["sex"]
        tmp["organismPart"] = specimen["organismPart"]
        tmp["lat"] = specimen["lat"]
        tmp["lng"] = specimen["lon"]
        tmp["locality"] = specimen["locality"]
        sample["orgGeoList"].append(tmp)
    sample["tolid"] = list(sample["tolid"])

    # Collecting genome_notes
    sample["genome_notes"] = list()
    genome_notes_dict = {
        str(genome_note["tax_id"]): genome_note["articles"]
        for genome_note in genome_notes
    }
    if sample["tax_id"] == "876063_3126489":
        sample["genome_notes"] = genome_notes_dict["876063"]
    elif str(sample["tax_id"]) in genome_notes_dict:
        sample["genome_notes"] = genome_notes_dict[str(sample["tax_id"])]

    # Collecting goat_info
    sample["goat_info"] = None
    try:
        if sample["tax_id"] == "876063_3126489":
            goat_response = requests.get(
                "https://goat.genomehubs.org/api/v0.0.1/record?recordId=3126489&result=taxon&taxonomy=ncbi",
                timeout=3600,
            ).json()
        else:
            goat_response = requests.get(
                f"https://goat.genomehubs.org/api/v0.0.1/record?recordId={sample['tax_id']}&result=taxon&taxonomy=ncbi",
                timeout=3600,
            ).json()
        if goat_response["records"]:
            goat_data = []
            goat_response = goat_response["records"][0]["record"]["attributes"]
            if "genome_size" in goat_response:
                goat_data.append(
                    {
                        "name": "genome_size",
                        "value": goat_response["genome_size"]["value"],
                        "count": goat_response["genome_size"]["count"],
                        "aggregation_method": goat_response["genome_size"][
                            "aggregation_method"
                        ],
                        "aggregation_source": goat_response["genome_size"][
                            "aggregation_source"
                        ],
                    }
                )
            if "busco_completeness" in goat_response:
                goat_data.append(
                    {
                        "name": "busco_completeness",
                        "value": goat_response["busco_completeness"]["value"],
                        "count": goat_response["busco_completeness"]["count"],
                        "aggregation_method": goat_response["busco_completeness"][
                            "aggregation_method"
                        ],
                        "aggregation_source": goat_response["busco_completeness"][
                            "aggregation_source"
                        ],
                    }
                )
            if sample["tax_id"] == "876063_3126489":
                sample["goat_info"] = {
                    "url": f"https://goat.genomehubs.org/records?record_id=3126489&result=taxon&taxonomy=ncbi#{sample['organism']}",
                    "attributes": goat_data,
                }
            else:
                sample["goat_info"] = {
                    "url": f"https://goat.genomehubs.org/records?record_id={sample['tax_id']}&result=taxon&taxonomy=ncbi#{sample['organism']}",
                    "attributes": goat_data,
                }
    except Exception as e:
        pass

    # collect other common names and nbnatlas links
    if sample["tax_id"] == "876063_3126489":
        nbn_atlas_response = requests.get(
            "https://portal.erga-biodiversity.eu/api/nbn_atlas/876063"
        ).json()
    else:
        nbn_atlas_response = requests.get(
            f"https://portal.erga-biodiversity.eu/api/nbn_atlas/{sample['tax_id']}"
        ).json()
    if len(nbn_atlas_response["results"]) > 0:
        if sample["commonName"] is None:
            sample["commonName"] = nbn_atlas_response["results"][0]["_source"][
                "commonName"
            ]
            sample["commonNameSource"] = nbn_atlas_response["results"][0]["_source"][
                "commonNameSource"
            ]
        sample["nbnatlas"] = nbn_atlas_response["results"][0]["_source"]["nbnatlas"]

    # Adding tol_qc links
    if sample["tax_id"] == "876063_3126489":
        tol_qc_response = requests.get(
            "https://portal.erga-biodiversity.eu/api/tol_qc/876063"
        ).json()
    else:
        tol_qc_response = requests.get(
            f"https://portal.erga-biodiversity.eu/api/tol_qc/{sample['tax_id']}"
        ).json()
    sample["show_tolqc"] = False
    if len(tol_qc_response["results"]) > 0:
        if bq_dataset_name == "dtol":
            for link in tol_qc_response["results"][0]["_source"]["tol_qc_links"]:
                if "darwin" in link:
                    sample["show_tolqc"] = True
                    sample["tolqc_links"] = [link]
        elif bq_dataset_name == "asg":
            for link in tol_qc_response["results"][0]["_source"]["tol_qc_links"]:
                if "asg" in link:
                    sample["show_tolqc"] = True
                    sample["tolqc_links"] = [link]
        else:
            sample["show_tolqc"] = True
            sample["tolqc_links"] = tol_qc_response["results"][0]["_source"][
                "tol_qc_links"
            ]

    # Remove duplicated records
    sample["records"] = remove_duplicated_metadata_records(sample["records"])

    # Remove duplicated runs
    sample["experiment"] = remove_duplicated_data_records(
        sample["experiment"], "run_accession"
    )

    # Remove duplicated assemblies
    sample["assemblies"] = remove_duplicated_data_records(
        sample["assemblies"], "accession"
    )

    # Remove duplicated symbionts assemblies
    sample["symbionts_assemblies"] = remove_duplicated_data_records(
        sample["symbionts_assemblies"], "accession"
    )

    # Remove duplicated metagenomes assemblies
    sample["metagenomes_assemblies"] = remove_duplicated_data_records(
        sample["metagenomes_assemblies"], "accession"
    )

    return beam.pvalue.TaggedOutput("normal", sample)


def build_dwh_record(element, bq_dataset_name, annotations):
    phylogenetic_ranks = (
        "kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "species",
    )
    sample = dict()

    # host metadata
    sample["tax_id"] = element[0]
    if sample["tax_id"] == "876063_3126489":
        response = requests.get("https://www.ebi.ac.uk/ena/browser/api/xml/3126489")
    else:
        response = requests.get(
            f"https://www.ebi.ac.uk/ena/browser/api/xml/{sample['tax_id']}"
        )
    root = etree.fromstring(response.content)
    try:
        sample["scientific_name"] = root.find("taxon").get("scientificName")
    except AttributeError:
        error_sample = {"tax_id": None, "scientific_name": None, "common_name": None}
        return beam.pvalue.TaggedOutput("error", error_sample)
    try:
        sample["common_name"] = root.find("taxon").get("commonName")
    except AttributeError:
        sample["common_name"] = element[1]["specimens"][0]["commonName"]
    if sample["common_name"] is None:
        sample["common_name"] = element[1]["specimens"][0]["commonName"]
    sample["current_status"] = "Submitted to BioSamples"
    sample["organisms"] = []

    for record in element[1]["specimens"]:
        el = dict()
        el["biosample_id"] = record["accession"]
        el["organism"] = record["organism"]["text"]
        el["common_name"] = record["commonName"]
        el["sex"] = record["sex"]
        el["organism_part"] = record["organismPart"]
        el["latitude"] = record["lat"]
        el["longitude"] = record["lon"]
        el["project_name"] = record["project_name"]
        el["lifestage"] = record["lifestage"]
        el["habitat"] = record["habitat"]
        sample["organisms"].append(el)

    sample["specimens"] = []

    sample["raw_data"], sample["assemblies"], sample["project_name"] = (
        parse_data_records_dwh(element[1]["specimens"])
    )

    # adding phylogenetic information
    sample["phylogenetic_tree"] = dict()
    for rank in phylogenetic_ranks:
        sample["phylogenetic_tree"][rank] = {
            "scientific_name": "Not specified",
            "common_name": "Not specified",
        }

    try:
        for taxon in root.find("taxon").find("lineage").findall("taxon"):
            rank = taxon.get("rank")
            if rank in phylogenetic_ranks:
                scientific_name = taxon.get("scientificName")
                common_name = taxon.get("commonName")
                sample["phylogenetic_tree"][rank]["scientific_name"] = (
                    scientific_name if scientific_name else "Not specified"
                )
                sample["phylogenetic_tree"][rank]["common_name"] = (
                    common_name if common_name else "Not specified"
                )
    except AttributeError:
        error_sample = {
            "tax_id": sample["tax_id"],
            "scientific_name": sample["organism"],
            "common_name": sample["commonName"],
        }
        return beam.pvalue.TaggedOutput("error", error_sample)

    if sample["tax_id"] in [624, 1773, 2697049]:
        error_sample = {"tax_id": None, "scientific_name": None, "common_name": None}
        return beam.pvalue.TaggedOutput("error", error_sample)

    # update phylogenetic tree names
    sample["phylogenetic_tree_scientific_names"] = list()
    sample["phylogenetic_tree_common_names"] = list()
    for rank in phylogenetic_ranks:
        sample["phylogenetic_tree_scientific_names"].append(
            sample["phylogenetic_tree"][rank]["scientific_name"]
        )
        sample["phylogenetic_tree_common_names"].append(
            sample["phylogenetic_tree"][rank]["common_name"]
        )

    # symbionts and metagenomes raw data
    sample["symbionts_raw_data"], sample["symbionts_assemblies"], _ = (
        parse_data_records_dwh(element[1]["symbionts"])
    )
    sample["symbionts"] = []
    for symbiont in element[1]["symbionts"]:
        el = dict()
        el["biosample_id"] = symbiont["accession"]
        el["organism"] = symbiont["organism"]["text"]
        el["common_name"] = symbiont["commonName"]
        el["sex"] = symbiont["sex"]
        el["organism_part"] = symbiont["organismPart"]
        sample["symbionts"].append(el)
    sample["metagenomes_raw_data"], sample["metagenomes_assemblies"], _ = (
        parse_data_records_dwh(element[1]["metagenomes"])
    )
    sample["metagenomes"] = []
    for metagenome in element[1]["metagenomes"]:
        el = dict()
        el["biosample_id"] = metagenome["accession"]
        el["organism"] = metagenome["organism"]["text"]
        el["common_name"] = metagenome["commonName"]
        el["sex"] = metagenome["sex"]
        el["organism_part"] = metagenome["organismPart"]
        sample["metagenomes"].append(el)

    sample["symbionts_status"] = "Not available"
    if len(sample["symbionts"]) > 0:
        sample["symbionts_status"] = "Symbionts Submitted to BioSamples"
    if len(sample["symbionts_assemblies"]) > 0:
        sample["symbionts_status"] = "Symbionts Assemblies - Submitted"

    sample["metagenomes_status"] = "Not available"
    if len(sample["metagenomes"]) > 0:
        sample["metagenomes_status"] = "Metagenomes Submitted to BioSamples"
    if len(sample["metagenomes_assemblies"]) > 0:
        sample["metagenomes_status"] = "Metagenomes Assemblies - Submitted"

    # host data status
    sample["current_status"] = (
        "Raw Data - Submitted"
        if len(sample["raw_data"]) != 0
        else sample["current_status"]
    )
    sample["current_status"] = (
        "Assemblies - Submitted"
        if len(sample["assemblies"]) != 0
        else sample["current_status"]
    )

    annotations_dict = {
        str(annotation["tax_id"]): annotation["annotations"]
        for annotation in annotations
    }
    if (
        sample["tax_id"] == "876063_3126489"
        or str(sample["tax_id"]) in annotations_dict
    ):
        sample["current_status"] = "Annotation Complete"

    if sample["tax_id"] == "876063_3126489":
        sample["tax_id"] = 3126489

    return beam.pvalue.TaggedOutput("normal", sample)
