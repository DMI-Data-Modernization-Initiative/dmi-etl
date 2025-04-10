with malaria_pcr_species_cte as (

    SELECT 
    malaria_pcr_species.pid,
    malaria_pcr_species.consent,
       CASE 
        WHEN "PCR_MalariaSpecies" = 'Plasmodium' THEN 3
        WHEN "PCR_MalariaSpecies" = 'Plasmodium,P. falciparum' THEN 1
        WHEN "PCR_MalariaSpecies" = 'Plasmodium,P. falciparum,P. vivax' THEN 2
        ELSE NULL
    END AS pcr_malaria_species_code,
    CASE 
        WHEN "PCR_MalariaSpecies" = 'Plasmodium' THEN 'Plasmodium unspeciated'
        WHEN "PCR_MalariaSpecies" = 'Plasmodium,P. falciparum' THEN 'P. falciparum'
        WHEN "PCR_MalariaSpecies" = 'Plasmodium,P. falciparum,P. vivax' THEN 'P. falciparum, P. vivax'
        ELSE NULL
    END AS pcr_malaria_species

 from {{ ref('stg_afi_surveillance') }}  as malaria_pcr_species
) 
SELECT 
    pid,
    consent,
    pcr_malaria_species_code,
    pcr_malaria_species
    
FROM malaria_pcr_species_cte