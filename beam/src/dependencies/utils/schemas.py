bq_metadata_schema = {
    "fields": [
        {
            "name": "tax_id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "scientific_name",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "common_name",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "current_status",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "organisms",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "biosample_id",
                    "type": "STRING",
                    "mode": "REQUIRED"
                },
                {
                    "name": "organism",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "common_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sex",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "organism_part",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "latitude",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "longitude",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "project_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "lifestage",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "habitat",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }
            ]
        },
        {
            "name": "specimens",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "biosample_id",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "organism",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "common_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sex",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "organism_part",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "latitude",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "longitude",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }
            ]
        },
        {
            "name": "phylogenetic_tree",
            "type": "RECORD",
            "mode": "REQUIRED",
            "fields": [
                {
                    "name": "kingdom",
                    "type": "RECORD",
                    "mode": "REQUIRED",
                    "fields": [
                        {
                            "name": "scientific_name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "common_name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                    ]
                },
                {
                    "name": "phylum",
                    "type": "RECORD",
                    "mode": "REQUIRED",
                    "fields": [
                        {
                            "name": "scientific_name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "common_name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                    ]
                },
                {
                    "name": "class",
                    "type": "RECORD",
                    "mode": "REQUIRED",
                    "fields": [
                        {
                            "name": "scientific_name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "common_name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                    ]
                },
                {
                    "name": "order",
                    "type": "RECORD",
                    "mode": "REQUIRED",
                    "fields": [
                        {
                            "name": "scientific_name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "common_name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                    ]
                },
                {
                    "name": "family",
                    "type": "RECORD",
                    "mode": "REQUIRED",
                    "fields": [
                        {
                            "name": "scientific_name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "common_name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                    ]
                },
                {
                    "name": "genus",
                    "type": "RECORD",
                    "mode": "REQUIRED",
                    "fields": [
                        {
                            "name": "scientific_name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "common_name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                    ]
                },
                {
                    "name": "species",
                    "type": "RECORD",
                    "mode": "REQUIRED",
                    "fields": [
                        {
                            "name": "scientific_name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "common_name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                    ]
                },
            ]
        },
        {
            "name": "phylogenetic_tree_scientific_names",
            "type": "STRING",
            "mode": "REPEATED"
        },
        {
            "name": "phylogenetic_tree_common_names",
            "type": "STRING",
            "mode": "REPEATED"
        },
        {
            "name": "symbionts",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "biosample_id",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "organism",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "common_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sex",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "organism_part",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }
            ]
        },
        {
            "name": "metagenomes",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "biosample_id",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "organism",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "common_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sex",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "organism_part",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }
            ]
        },
        {
            "name": "raw_data",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "study_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "secondary_study_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sample_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "secondary_sample_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "experiment_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "run_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submission_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "tax_id",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "scientific_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "instrument_platform",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "instrument_model",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "library_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "nominal_length",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "library_layout",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "library_strategy",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "library_source",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "library_selection",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "read_count",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "base_count",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "center_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "first_public",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "last_updated",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "experiment_title",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "study_title",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "study_alias",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "experiment_alias",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "run_alias",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "fastq_bytes",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "fastq_md5",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "fastq_ftp",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "fastq_aspera",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "fastq_galaxy",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_bytes",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_md5",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_ftp",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_aspera",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_galaxy",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_format",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sra_bytes",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sra_md5",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sra_ftp",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sra_aspera",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sra_galaxy",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sample_alias",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "broker_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sample_title",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }, {
                    "name": "nominal_sdev",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }, {
                    "name": "first_created",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }, {
                    "name": "library_construction_protocol",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },

            ]
        },
        {
            "name": "assemblies",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "description",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }
            ]
        },
        {
            "name": "symbionts_raw_data",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "study_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "secondary_study_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sample_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "secondary_sample_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "experiment_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "run_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submission_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "tax_id",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "scientific_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "instrument_platform",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "instrument_model",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "library_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "nominal_length",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "library_layout",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "library_strategy",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "library_source",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "library_selection",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "read_count",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "base_count",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "center_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "first_public",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "last_updated",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "experiment_title",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "study_title",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "study_alias",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "experiment_alias",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "run_alias",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "fastq_bytes",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "fastq_md5",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "fastq_ftp",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "fastq_aspera",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "fastq_galaxy",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_bytes",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_md5",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_ftp",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_aspera",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_galaxy",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_format",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sra_bytes",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sra_md5",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sra_ftp",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sra_aspera",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sra_galaxy",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sample_alias",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "broker_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sample_title",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }, {
                    "name": "nominal_sdev",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }, {
                    "name": "first_created",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }, {
                    "name": "library_construction_protocol",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },

            ]
        },
        {
            "name": "symbionts_assemblies",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "description",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }
            ]
        },
        {
            "name": "metagenomes_raw_data",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "study_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "secondary_study_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sample_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "secondary_sample_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "experiment_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "run_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submission_accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "tax_id",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "scientific_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "instrument_platform",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "instrument_model",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "library_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "nominal_length",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "library_layout",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "library_strategy",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "library_source",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "library_selection",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "read_count",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "base_count",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "center_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "first_public",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "last_updated",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "experiment_title",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "study_title",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "study_alias",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "experiment_alias",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "run_alias",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "fastq_bytes",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "fastq_md5",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "fastq_ftp",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "fastq_aspera",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "fastq_galaxy",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_bytes",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_md5",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_ftp",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_aspera",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_galaxy",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "submitted_format",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sra_bytes",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sra_md5",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sra_ftp",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sra_aspera",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sra_galaxy",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sample_alias",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "broker_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "sample_title",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }, {
                    "name": "nominal_sdev",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }, {
                    "name": "first_created",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }, {
                    "name": "library_construction_protocol",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },

            ]
        },
        {
            "name": "metagenomes_assemblies",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "accession",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "description",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }
            ]
        },
        {
            "name": "symbionts_status",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "metagenomes_status",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "project_name",
            "type": "STRING",
            "mode": "REPEATED"
        }
    ]
}