-- below is uself to drop all datasets. Note change suffix depending on your profiles.yml
bq rm -r -d -f jgdw_raw_archive
bq rm -r -d -f jgdw_raw_clean
bq rm -r -d -f jgdw_raw_eshop_shopify
bq rm -r -d -f jgdw_raw_netsuite
bq rm -r -d -f jgdw_pl_reference
bq rm -r -d -f jgdw_pl_reference_stage
bq rm -r -d -f jgdw_pl_sales
