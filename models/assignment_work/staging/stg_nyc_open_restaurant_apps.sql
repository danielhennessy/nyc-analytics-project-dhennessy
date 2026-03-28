-- Clean and standardize NYC Open Restaurant applications data
-- One row per application record

WITH source AS (
    SELECT * FROM {{ source('raw', 'source_nyc_open_restaurant_apps') }}
),

cleaned AS (
    SELECT
        -- Keep all other columns unless transformed below
        * EXCEPT (
            objectid,
            globalid,
            seating_interest_sidewalk_roadway_both,
            restaurant_name,
            legal_business_name,
            doing_business_as_dba,
            building_number,
            street,
            borough,
            zip,
            business_address,
            food_service_establishment_permit,
            sidewalk_dimensions_length,
            sidewalk_dimensions_width,
            sidewalk_dimensions_area,
            roadway_dimensions_length,
            roadway_dimensions_width,
            roadway_dimensions_area,
            approved_for_sidewalk_seating,
            approved_for_roadway_seating,
            qualify_alcohol,
            landmark_district_or_building,
            landmarkdistrict_terms,
            healthcompliance_terms,
            sla_serial_number,
            sla_license_type,
            time_of_submission,
            latitude,
            longitude,
            community_board,
            council_district,
            census_tract,
            bin,
            bbl,
            nta,
            food_service_establishment,
            seating_interest_sidewalk
        ),

        -- Identifiers
        CAST(objectid AS STRING) AS application_id,
        CAST(globalid AS STRING) AS global_id,

        -- Business details
        CAST(seating_interest_sidewalk_roadway_both AS STRING) AS seating_interest_sidewalk_roadway_both,
        CAST(seating_interest_sidewalk AS STRING) AS seating_interest_sidewalk,
        CAST(restaurant_name AS STRING) AS restaurant_name,
        CAST(legal_business_name AS STRING) AS legal_business_name,
        CAST(doing_business_as_dba AS STRING) AS doing_business_as_dba,
        CAST(food_service_establishment AS STRING) AS food_service_establishment,
        CAST(food_service_establishment_permit AS STRING) AS food_service_establishment_permit,

        -- Address
        CAST(building_number AS STRING) AS building_number,
        CAST(street AS STRING) AS street,

        CASE
            WHEN UPPER(TRIM(borough)) IN ('MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
            WHEN UPPER(TRIM(borough)) IN ('BRONX', 'THE BRONX') THEN 'Bronx'
            WHEN UPPER(TRIM(borough)) IN ('BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
            WHEN UPPER(TRIM(borough)) IN ('QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
            WHEN UPPER(TRIM(borough)) IN ('STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
            ELSE 'UNKNOWN'
        END AS borough,

        CASE
            WHEN UPPER(TRIM(CAST(zip AS STRING))) IN ('N/A', 'NA', '') THEN NULL
            WHEN REGEXP_CONTAINS(TRIM(CAST(zip AS STRING)), r'^\d{5}$') THEN TRIM(CAST(zip AS STRING))
            WHEN REGEXP_CONTAINS(TRIM(CAST(zip AS STRING)), r'^\d{9}$') THEN TRIM(CAST(zip AS STRING))
            WHEN REGEXP_CONTAINS(TRIM(CAST(zip AS STRING)), r'^\d{5}-\d{4}$') THEN TRIM(CAST(zip AS STRING))
            ELSE NULL
        END AS zip_code,

        CAST(business_address AS STRING) AS business_address,

        -- Dimensions / area fields
        CAST(sidewalk_dimensions_length AS STRING) AS sidewalk_dimensions_length,
        CAST(sidewalk_dimensions_width AS STRING) AS sidewalk_dimensions_width,
        CAST(sidewalk_dimensions_area AS STRING) AS sidewalk_dimensions_area,
        CAST(roadway_dimensions_length AS STRING) AS roadway_dimensions_length,
        CAST(roadway_dimensions_width AS STRING) AS roadway_dimensions_width,
        CAST(roadway_dimensions_area AS STRING) AS roadway_dimensions_area,

        -- Approval / compliance fields
        UPPER(TRIM(CAST(approved_for_sidewalk_seating AS STRING))) AS approved_for_sidewalk_seating,
        UPPER(TRIM(CAST(approved_for_roadway_seating AS STRING))) AS approved_for_roadway_seating,
        UPPER(TRIM(CAST(qualify_alcohol AS STRING))) AS qualify_alcohol,
        UPPER(TRIM(CAST(landmark_district_or_building AS STRING))) AS landmark_district_or_building,
        CAST(landmarkdistrict_terms AS STRING) AS landmarkdistrict_terms,
        CAST(healthcompliance_terms AS STRING) AS healthcompliance_terms,
        CAST(sla_serial_number AS STRING) AS sla_serial_number,
        CAST(sla_license_type AS STRING) AS sla_license_type,

        -- Date/time
        CAST(time_of_submission AS TIMESTAMP) AS time_of_submission,

        -- Geo
        CAST(latitude AS NUMERIC) AS latitude,
        CAST(longitude AS NUMERIC) AS longitude,
        CAST(community_board AS STRING) AS community_board,
        CAST(council_district AS STRING) AS council_district,
        CAST(census_tract AS STRING) AS census_tract,
        CAST(bin AS STRING) AS bin,
        CAST(bbl AS STRING) AS bbl,
        CAST(nta AS STRING) AS nta,

        -- Metadata
        CURRENT_TIMESTAMP() AS _stg_loaded_at

    FROM source

    -- Basic filters
    WHERE objectid IS NOT NULL
      AND borough IS NOT NULL

    -- Deduplicate
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY objectid
        ORDER BY time_of_submission DESC
    ) = 1
)

SELECT * FROM cleaned
