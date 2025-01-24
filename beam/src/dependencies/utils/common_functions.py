import apache_beam as beam

SPECIMENS_SYMBIONTS_CHECKLISTS = ["ERC000011", "ERC000053"]
METAGENOMES_CHECKLISTS = ["ERC000013", "ERC000024", "ERC000025", "ERC000047",
                          "ERC000050"]


def classify_samples(sample) -> beam.pvalue.TaggedOutput | dict[str, dict]:
    error_sample = dict()
    error_sample['biosample_id'] = sample["accession"]
    try:
        checklist = sample["characteristics"]["ENA-CHECKLIST"][0]["text"]
        if checklist in SPECIMENS_SYMBIONTS_CHECKLISTS:
            if ("symbiont" in sample["characteristics"]
                    and sample["characteristics"]["symbiont"][0][
                        "text"] == "Y"):
                return beam.pvalue.TaggedOutput("Symbionts", sample)
            else:
                return sample
        elif checklist in METAGENOMES_CHECKLISTS:
            return beam.pvalue.TaggedOutput("Metagenomes", sample)
        else:
            return sample
    except KeyError:
        return sample
