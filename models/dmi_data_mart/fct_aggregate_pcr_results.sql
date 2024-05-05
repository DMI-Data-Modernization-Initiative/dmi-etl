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