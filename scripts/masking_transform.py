import os
import json
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat, lit, expr, regexp_replace

def load_masking_rules(config_path: "config/masking_rules.json"):
    with open(config_path, 'r') as f:
        
        return json.load(f)

def apply_masking(df: DataFrame, rules_config: dict) -> DataFrame:
    "Aplicar las reglas de enmascacaramiento usando Spark SQL"
    fields = rules_config.get("fields",{})
    df_masked = df

    for field, rule, in fields.items():
        if field not in df.columns:
            continue

        rule_type = rule["type"]
        params = rule.get("params", {})

        if rule_type == "mask_transaction_id":
            visible = params.get("visible_digits", 4)
            mask_char = params.get("mask_char", "*")
            expr_mask = f"""
                CASE 
                    WHEN LENGTH({field}) is NULL THEN NULL 
                    
                    ELSE CONCAT (
                        REPEAT('{mask_char}', LENGTH(CAST({field} AS STRING)) - {visible}), 
                        RIGHT(CAST({field} AS STRING), {visible})
                    )
                END
            """
            df_masked = df_masked.withColumn(field, expr(expr_mask))
        else:
            print(f"Regla de enmascaramiento no reconocida: {rule_type}")
    return df_masked
