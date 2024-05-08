with unpivoted_tac_results as (
    {{
        dbt_utils.unpivot(
            relation=ref("intermediate_tac_results"),
            cast_to="text",
            exclude=['UniqueID', 'BarcodeNo', 'PIDServer', 'PIDNumber', 
            'SampleCollectionLocation', 'SampleCollectionDate', 'DateReceived', 'DateTested', 'Result'],
            field_name="disease",
            value_name="disease_result",
        )
    }}
)

select 
  pidnumber,
  barcodeno,
  result,
  case disease
  	when 'bartonella' then 'Bartonella'
  	when 's_pneumoniae' then 'S.pneumoniae'
  	when 'leptospira' then 'Leptospira'
  	when 'riftvalleyfever' then 'Rift Valley Fever'
  	when 'dengue' then 'Dengue fever'
  	when 'plasmodium' then 'Plasmodium'
  	when 'leishmania' then 'Leishmania'
  	when 'hiv_1' then 'HIV-1'
  	when 'b_pseudomallei' then 'B.pseudomallei'
  	when 'chikungunya' then 'Chikungunya'
  	when 'brucella' then 'Brucella'
  	when 'c_burnetii' then 'C.burnetii'
  	when 'salmonella' then 'Salmonella'
  	when 'rickettsia' then 'Rickettsia'
  	when 'p_vivax' then 'Plasmodium'
  	when 'p_falciparum' then 'Plasmodium'
  	when 'salmonellatyphi' then 'Salmonella'
  	else disease
  end as disease,
  disease_result,
  cast(current_date as date) as load_date
from unpivoted_tac_results