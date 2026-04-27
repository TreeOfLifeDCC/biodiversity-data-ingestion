from biodiv_airflow.config import BiodivConfig


def build_bq_warehouse_integration_sql(cfg: BiodivConfig) -> str:
    """
    Build the query for bp_integ_genome_biodiv_annotations.

    Enriches genome annotation summaries with taxonomy, occurrences,
    spatial features, range estimates, elevation, and provenance URLs.

    This version uses a pivot table to unpack gene/transcript_biotypes into columns
    and calculate proportions. This enables filtering based on the proportion of
    gene_biotypes in the genotype.
    """
    query = f"""
        CREATE OR REPLACE TABLE `{cfg.gcp_project}.{cfg.bq_dataset}.bp_integ_genome_biodiv_annotations` AS
        
        WITH genome_annotations_count AS (
          SELECT g.*
          FROM `{cfg.gcp_project}.{cfg.bq_dataset}.bp_genome_biotype_summary` AS g
          INNER JOIN (
            SELECT DISTINCT accession
            FROM `{cfg.gcp_project}.{cfg.bq_dataset}.bp_taxonomy_validated`
          ) AS t
            ON g.accession = t.accession
        ),
        -- ---------
        /*
        Generating a struct with gene biotypes as columns
        */
        
        matrix_gene_biotype AS (
          WITH unpacked_biotypes AS (
            SELECT
              accession,
              gb.gene_biotype,
              gb.gene_biotype_percentage
          FROM genome_annotations_count AS gf,
          UNNEST(gf.gene_biotypes) AS gb
          )
          SELECT *
          FROM unpacked_biotypes
          PIVOT(
            MAX(gene_biotype_percentage)
            FOR gene_biotype IN (
              'IG_C_gene', 'IG_D_gene', 'IG_J_gene', 'IG_V_gene',
              'Mt_rRNA', 'Mt_tRNA',
              'TR_C_gene', 'TR_J_gene', 'TR_V_gene',
              'Y_RNA',
              'antisense',
              'lncRNA',
              'miRNA',
              'misc_RNA',
              'processed_pseudogene', 'protein_coding', 'pseudogene',
              'rRNA', 'ribozyme', 'scaRNA', 'snRNA', 'snoRNA',
              'tRNA', 'transcribed_processed_pseudogene', 'transcribed_unprocessed_pseudogene',
              'unitary_pseudogene', 'unprocessed_pseudogene',
              'vault_RNA'
            )
          )
        ),
        
        gene_matrix_struct AS (
          SELECT
            accession,
              STRUCT(
                IG_C_gene, IG_D_gene, IG_J_gene, IG_V_gene,
                Mt_rRNA, Mt_tRNA,
                TR_C_gene, TR_J_gene, TR_V_gene,
                Y_RNA,
                antisense,
                lncRNA,
                miRNA,
                misc_RNA,
                processed_pseudogene, protein_coding, pseudogene,
                rRNA, ribozyme, scaRNA, snRNA, snoRNA,
                tRNA, transcribed_processed_pseudogene, transcribed_unprocessed_pseudogene,
                unitary_pseudogene, unprocessed_pseudogene,
                vault_RNA
              ) AS gbiotype_matrix
          FROM matrix_gene_biotype
        ),
        -- --------
        
        transformed_taxonomy AS (
          SELECT
            accession,
              STRUCT( 
                tax.kingdom,
                tax.phylum,
                tax.class,
                tax.order,
                tax.family,
                tax.genus,
                tax.species,
                tax.tax_id,
                tax.gbif_usageKey,
                tax.gbif_status
              ) AS taxonomy
          FROM
            `{cfg.gcp_project}.{cfg.bq_dataset}.bp_taxonomy_validated` AS tax
        ),
        
        transformed_gbif AS (
          SELECT
            accession,
            ARRAY_AGG(
              STRUCT(
                gb.occurrenceID,
                SAFE.ST_GEOGPOINT(gb.decimalLongitude, gb.decimalLatitude) AS geo_coordinate,
                gb.geodeticDatum,
                gb.coordinateUncertaintyInMeters,
                gb.eventDate,
                CAST(gb.elevation AS FLOAT64) AS elevation, -- Ad hoc: delete for the next run of the pipeline.
                gb.countryCode,
                gb.iucnRedListCategory,
                gb.gadm,
                gb.institutionCode,
                gb.collectionCode,
                gb.catalogNumber,
                gb.datasetKey,
                gb.license
              )
            ) AS gbif_occs
          FROM `{cfg.gcp_project}.{cfg.bq_dataset}.bp_gbif_occurrences` AS gb
          GROUP BY accession
        ),
        
        spatial_annotations AS (
          SELECT
            * EXCEPT(species, tax_id)
          FROM `{cfg.gcp_project}.{cfg.bq_dataset}.bp_spatial_annotations`
        ),
        range_sizes AS (
            SELECT
              * EXCEPT(species)
            FROM `{cfg.gcp_project}.{cfg.bq_dataset}.bp_species_range_estimates`
        ),
        altitudinal_profile AS (
          SELECT
            accession,
            ROUND(AVG(CAST(elevation AS FLOAT64)), 2) AS mean_elevation,
            MIN(CAST(elevation AS FLOAT64)) AS min_elevation,
            MAX(CAST(elevation AS FLOAT64)) AS max_elevation,
            ROUND(APPROX_QUANTILES(CAST(elevation AS FLOAT64), 2)[OFFSET(1)], 2) AS median_elevation--,
            --ARRAY_AGG(CAST(elevation AS FLOAT64)) AS vals
          FROM `{cfg.gcp_project}.{cfg.bq_dataset}.bp_gbif_occurrences`
          WHERE elevation IS NOT NULL
          GROUP BY accession
        ),
        -- ------
        /*
        Adding URLs for GTF files, Ensembl & GBIF
        */
        
        metadata_struct AS (
         SELECT
            accession,
              STRUCT(
                bp.Biodiversity_portal,
                bp.Ensembl_browser,
                bp.GTF,
                bp.gbif_url AS GBIF
            ) AS meta_urls
          FROM `{cfg.gcp_project}.{cfg.bq_dataset}.bp_provenance_metadata` AS bp
        )
        
        -- ------
        SELECT
           gc.*,
           t.* EXCEPT(accession),
           g.* EXCEPT(accession),
           s.* EXCEPT(accession),
           r.* EXCEPT(accession),
           alt.* EXCEPT(accession),
           gmax.* EXCEPT(accession),
           urls.* EXCEPT(accession)
        FROM genome_annotations_count AS gc
        LEFT JOIN transformed_taxonomy AS t
          ON gc.accession = t.accession
        LEFT JOIN transformed_gbif AS g
          ON gc.accession = g.accession
        LEFT JOIN spatial_annotations AS s
          ON gc.accession = s.accession
        LEFT JOIN range_sizes AS r
          ON gc.accession = r.accession
        LEFT JOIN altitudinal_profile as alt
          ON gc.accession = alt.accession
        LEFT JOIN gene_matrix_struct AS gmax
          ON gc.accession = gmax.accession
        LEFT JOIN metadata_struct AS urls
          ON gc.accession = urls.accession
        """

    return query


def build_bq_genome_annotations_summary_sql(cfg: BiodivConfig) -> str:
    """
    Return SQL to build bp_genome_biotype_summary.
    Aggregates gene and transcript biotype counts and percentages by accession.
    """
    
    query = f"""
    CREATE OR REPLACE TABLE `{cfg.gcp_project}.{cfg.bq_dataset}.bp_genome_biotype_summary` AS

    WITH accessions AS (
      SELECT DISTINCT accession
      FROM `{cfg.gcp_project}.{cfg.bq_dataset}.annotations`
    ),
    
    reduced_gene_biotypes AS (
      SELECT DISTINCT accession,
        gene_id,
        gene_biotype
      FROM `{cfg.gcp_project}.{cfg.bq_dataset}.annotations`
    ),
    
    count_gene_biotypes AS (
      SELECT
        accession,
        gene_biotype,
        COUNT(*) AS gene_biotype_count
      FROM reduced_gene_biotypes
      GROUP BY accession, gene_biotype
    ),
    
    total_gene_count AS (
      SELECT
        accession,
        SUM(gene_biotype_count) AS total_gene_biotypes
      FROM count_gene_biotypes
      GROUP BY accession
    ),
    
    gene_proportions AS (
      SELECT
        g.accession,
        g.gene_biotype,
        g.gene_biotype_count,
        t.total_gene_biotypes,
        ROUND((gene_biotype_count / total_gene_biotypes) * 100, 3) AS gene_biotype_percentage
      FROM count_gene_biotypes AS g
      LEFT JOIN total_gene_count AS t
      USING(accession)
    ),
    
    gene_biotype_struct AS (
      SELECT
        accession,
        ARRAY_AGG(
          STRUCT(
            gene_biotype,
            gene_biotype_count,
            total_gene_biotypes,
            gene_biotype_percentage
          )
          ORDER BY gene_biotype
       ) AS gene_biotypes
      FROM gene_proportions
      GROUP BY accession
    ),
    
    reduced_transcript_biotypes AS (
      SELECT
        DISTINCT accession,
        transcript_id,
        transcript_biotype
      FROM `{cfg.gcp_project}.{cfg.bq_dataset}.annotations`
    ),
    
    count_transcript_biotypes AS (
      SELECT
        accession,
        transcript_biotype,
        COUNT(*) AS transcript_biotype_count
      FROM reduced_transcript_biotypes
      GROUP BY accession, transcript_biotype
    ),
    
    total_trans_count AS (
      SELECT
        accession,
        SUM(transcript_biotype_count) AS total_trans_biotypes
      FROM count_transcript_biotypes
      GROUP BY accession
    ),
    
    trans_proportions AS (
      SELECT
        t.accession,
        t.transcript_biotype,
        t.transcript_biotype_count,
        tc.total_trans_biotypes,
        ROUND((transcript_biotype_count / total_trans_biotypes) * 100, 3) AS trans_biotype_percentage
      FROM count_transcript_biotypes AS t
      LEFT JOIN total_trans_count AS tc
      USING(accession)
    ),
    
    transcript_biotype_struct AS (
      SELECT
        accession,
        ARRAY_AGG(
          STRUCT(
            transcript_biotype,
            transcript_biotype_count,
            total_trans_biotypes,
            trans_biotype_percentage
          )
          ORDER BY transcript_biotype
      ) AS transcript_biotypes
      FROM trans_proportions
      GROUP BY accession
    )
    
    SELECT
      a.accession,
      gbs.gene_biotypes,
      tbs.transcript_biotypes
    FROM accessions AS a
    LEFT JOIN gene_biotype_struct AS gbs
      ON a.accession = gbs.accession
    LEFT JOIN transcript_biotype_struct AS tbs
      ON a.accession = tbs.accession

    """
    return query

