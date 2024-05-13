with data as (
    select 
        "Unique_ID" as UniqueID, 
        "Barcode No" as BarcodeNo, 
        "PIDserver" as PIDServer, 
        "PIDNumber" as PIDNumber, 
        "Sample Collection Location" as SampleCollectionLocation, 
        sample_collection_date as SampleCollectionDate, 
        "Date_Received" as DateReceived, 
        "Date Tested" as DateTested, 
        "Result" as Result,  
        "Plasmodium.1" AS Plasmodium, 
        "Dengue.1" AS Dengue, 
        "HIV-1.1" AS HIV_1, 
        "Rickettsia.1" AS Rickettsia, 
        "Bartonella.1" AS Bartonella, 
        "Chikungunya.1" AS Chikungunya, 
        "Salmonella.1" AS Salmonella, 
        "Leishmania.1" AS Leishmania, 
        "Leptospira.1" AS Leptospira, 
        "Salmonella Typhi.1" AS SalmonellaTyphi, 
        "Rift Valley Fever.1" AS RiftValleyFever, 
        "Brucella.1" AS Brucella, 
        "C. burnetii.1" AS C_burnetii, 
        "P. falciparum.1" AS P_falciparum, 
        "S. pneumoniae.1" AS S_pneumoniae, 
        "P. vivax.1" AS P_vivax, 
        "B. pseudomallei.1" AS B_pseudomallei
    from {{ ref('stg_afi_tac_results') }}
),
co_infection_data as (
	select 
		pidnumber,
		concat_ws(', ', 
	   	case when p_falciparum = 'POS' and p_vivax <> 'POS' then 'P.falciparum' else null end,
		case when p_falciparum <> 'POS' and p_vivax = 'POS' then 'P.vivax' else null end,
		case when p_falciparum = 'POS' and p_vivax = 'POS' then 'Mixed infection' else null end,
		case when plasmodium = 'POS' and p_falciparum <> 'POS' and p_vivax <> 'POS' then 'Other plasmodium' else null end,
	    case when dengue = 'POS' then 'Dengue' else null end,
	    case when hiv_1 = 'POS' then 'HIV-1' else null end,
	    case when rickettsia = 'POS' then 'Rickettsia' else null end,
	    case when bartonella = 'POS' then 'Bartonella' else null end,
	    case when chikungunya = 'POS' then 'Chikungunya' else null end,
	    case when salmonella = 'POS' then 'Salmonella' else null end,
	    case when leishmania = 'POS' then 'Leishmania' else null end,
	    case when leptospira = 'POS' then 'Leptospira' else null end,
	    case when salmonellatyphi = 'POS' then 'Salmonella Typhi' else null end,
	    case when riftvalleyfever = 'POS' then 'Rift Valley Fever' else null end,
	    case when brucella = 'POS' then 'Brucella' else null end,
	    case when c_burnetii = 'POS' then 'C.burnetii' else null end,
	    case when s_pneumoniae = 'POS' then 'S.pneumoniae' else null end,
	    case when b_pseudomallei = 'POS' then 'B.pseudomallei' else null end  
	    ) as co_infection
	from data
	where 
	   	case when p_falciparum = 'POS'  then 1 else 0 end +
		case when p_falciparum <> 'POS' and p_vivax = 'POS' then 1 else 0 end +
		case when p_falciparum = 'POS' and p_vivax = 'POS' then 1 else 0 end +
		case when plasmodium = 'POS' and p_falciparum <> 'POS' and p_vivax <> 'POS' then 1 else 0 end +
	    case when dengue = 'POS' then 1 else 0 end +
	    case when hiv_1 = 'POS' then 1 else 0 end +
	    case when rickettsia = 'POS' then 1 else 0 end +
	    case when bartonella = 'POS' then 1 else 0 end +
	    case when chikungunya = 'POS' then 1 else 0 end +
	    case when salmonella = 'POS' then 1 else 0 end +
	    case when leishmania = 'POS' then 1 else 0 end +
	    case when leptospira = 'POS' then 1 else 0 end +
	    case when salmonellatyphi = 'POS' then 1 else 0 end +
	    case when riftvalleyfever = 'POS' then 1 else 0 end +
	    case when brucella = 'POS' then 1 else 0 end +
	    case when c_burnetii = 'POS' then 1 else 0 end +
	    case when s_pneumoniae = 'POS' then 1 else 0 end +
	    case when b_pseudomallei = 'POS' then 1 else 0 end
	 > 1
)
select 
    data.*,
    co_infection_data.co_infection
from data 
left join co_infection_data on co_infection_data.pidnumber = data.pidnumber
