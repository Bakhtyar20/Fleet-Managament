# Databricks notebook source
# MAGIC %run Pricing/pdm_functions

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %md #### Import libraries

# COMMAND ----------

from pyspark.sql.functions import col,max,lit,when
from pyspark.sql import DataFrame
from datetime import datetime, date
from delta.tables import *

# COMMAND ----------

# MAGIC %md ### Column list reference

# COMMAND ----------

columns_ref = spark.sql("SELECT * FROM pricing.pdm_contract_full LIMIT 1").columns

# COMMAND ----------

# MAGIC %md # Fleet

# COMMAND ----------

# MAGIC %md ## Pricing FLeet Automated

# COMMAND ----------

df_fleet = spark.sql(
    """
    SELECT 
        KEY_CONTRACT,
        CD_DAY_RETURN AS EST_RETURN_DATE,
        ACCOUNT_NAME AS CUSTOMER_NAME,
        CD_COMPANY AS SUBSIDIARY_CODE,
        CD_CONTRACT AS CONTRACT_NUMBER,
        BODYGROUP_LOCAL AS VEHICLE_BODY_LOCAL_CODE,
        CONTRACTUAL_KM,
        CONTRACT_DURATION,
        CONTRACT_DURATION / 30 AS EST_TOTAL_DURATION,
        HOLDING_CONTRACT_TYPE AS PRODUCT_TYPE_HOLDING,
        AVG_PRICE_OPTIONAL AS PRICE_OPTIONAL_AM_LOC,
        HOLDING_CONTRACT_STATUS AS CONTRACT_STATUS_HOLDING,
        HOLDING_CUSTOMERPROFILE as CUSTOMER_LEGAL_TYPE,
        BUY_BACK AS BUY_BACK_AGREEMENT_FLAG,
        LEASE_BACK AS USED_CAR_LEASE_FLAG,
        SEATNB AS SEAT_NUMBER,
        AVG_PRICE_VEHICLE AS PRICE_VEHICLE_AM_LOC,
        AVG_DISCOUNT_PRICE AS PRICE_VEHICLE_DISC_AM_LOC,
        CONTRACT_STATUS_LOCAL AS CONTRACT_STATUS_LOCAL_CODE,
        GEAR_BOX_LOCAL AS TRANSMISSION_LOCAL_CODE,
        CC,
        AVG_OVERDUE_DAYS AS OVERDUE_DAYS_CURRENT,
        START_KM,
        BODY_GROUP AS VEHICLE_BODY_HOLDING,
        DESC_VEHICLETYPE AS VEHICLE_TYPE_HOLDINGVEH_STATUS_CD,
        VEHICLETYPE_LOCAL AS VEHICLE_TYPE_LOCAL_CODE,
        DESC_MODDESCRIPTION AS MODEL_DESCRIPTION,
        DATE_INS,
        CO2_EMISSIONS,
        CD_PLATE AS PLATE,
        DESC_MAKE AS MAKE_HOLDING,
        CD_DOORS_NUMBER AS DOORS_NUMBER,
        DESC_FUELTYPE AS FUEL_TYPE_HOLDING,
        DESC_COMPANY,
        DESC_COUNTRY AS COUNTRY,
        CD_CURRENCY,
        GEAR_BOX AS TRANSMISSION_HOLDING,
        DATE_REFRESH,
        VAT AS VAT_PCT,
        THA_NORMALIZED_NAME,
        INSCOPE,
        NETBOOKVALUEAMOUNT,
        RV_BASE_AMOUNT AS RV_BASE_AM_LOC,
        VEH_STATUS_CD AS VEHICLE_STATUS_LOCAL_CODE,
        DESC_COMPANY AS SUBSIDIARY_NAME,
        DATE_FROM AS SNAPSHOT_PERIOD,
        MIGRATED AS MIGRATED_FLAG,
        CONTRACT_TYPE_LOCAL AS PRODUCT_TYPE_LOCAL_CODE,
        FUNDED_FLEET,
        MODEL_LOCAL AS MAKE_LOCAL_CODE,
        DESC_GENERIC_MODEL AS MODEL_HOLDING,
        DESC_MAKETSECTOR AS MARKET_SECTOR_HOLDING,
        FUEL_LOCAL AS FUEL_TYPE_LOCAL_CODE,
        DIN AS DIN_HORSEPOWER,
        ORDERING_DATE AS ORDER_DATE,
        CD_DAY_START AS START_DATE,
        COLOR AS VEHICLE_COLOR,
        (START_KM + CONTRACTUAL_KM) AS EST_TOTAL_KM,
        FUEL_PREDICTED AS FUEL_TYPE_PREDICTED,
        ISOVERDUE AS OVERDUE_FLAG,
        VEH_STATUS_DESC AS VEHICLE_STATUS_HOLDING,
        'FLEET' AS SOURCE_TABLE,
        VIN
    FROM 
        pricing.pdm_fleet
    """
)

# COMMAND ----------

# MAGIC %md #### Get the list of subsidiaries that are automated so they will not be fetched from the manual fleet

# COMMAND ----------

subsidiaries_list = df_fleet.select("SUBSIDIARY_CODE").distinct().rdd.map(lambda row: row[0]).collect()
subsidiaries_list_str = ','.join(f"'{str(subsidiary)}'" for subsidiary in subsidiaries_list)

# COMMAND ----------

# MAGIC %md ## Pricing Fleet Manual

# COMMAND ----------

# MAGIC %md ### Get maximum campign per subsidiary

# COMMAND ----------

df_fleet_csv_campaign = spark.sql( """WITH max_cmp as (
                                    Select CD_COMPANY, MAX(CAMPAIGN) as Max_Campaign 
                                    FROM pricing.pdm_fleet_csv 
                                    WHERE CD_COMPANY NOT IN (""" + subsidiaries_list_str + """) 
                                    group by CD_COMPANY
                                    ) select concat(CD_COMPANY,  '_', Max_Campaign) as Company_campaign from  max_cmp
                                    """)

max_campaign_list = df_fleet_csv_campaign.select("Company_campaign").distinct().rdd.map(lambda row: row[0]).collect()
max_campaign_list_str = ','.join(f"'{str(company_campaign)}'" for company_campaign in max_campaign_list)
print(max_campaign_list_str)

# COMMAND ----------

df_fleet_csv = (
    spark.sql(
    """
    SELECT 
        CONCAT(CD_COMPANY, '_', CD_CONTRACT) AS KEY_CONTRACT , 
        CD_DAY_RETURN AS EST_RETURN_DATE, 
        CD_COMPANY AS SUBSIDIARY_CODE, 
        CD_CONTRACT AS CONTRACT_NUMBER, 
        CC, 
        DIN AS DIN_HORSEPOWER, 
        DESC_MAKETSECTOR AS MARKET_SECTOR_HOLDING, 
        AVG_OVERDUE_DAYS AS OVERDUE_DAYS_CURRENT, 
        START_KM,
        CAMPAIGN, 
        CONTRACTUAL_KM, 
        AVG_PRICE_VEHICLE AS PRICE_VEHICLE_AM_LOC, 
        AVG_DISCOUNT_PRICE AS PRICE_VEHICLE_DISC_AM_LOC, 
        AVG_PRICE_OPTIONAL AS PRICE_OPTIONAL_AM_LOC, 
        CONTRACT_DURATION, 
        CONTRACT_DURATION / 30 AS EST_TOTAL_DURATION, 
        ISOVERDUE AS OVERDUE_FLAG,
        HOLDING_CONTRACT_TYPE AS PRODUCT_TYPE_HOLDING, 
        IF(HOLDING_CONTRACT_TYPE IN ('FULL OPERATIONAL LEASING', 'FINANCIAL LEASE', 'POOL FLEET'), 'Y', 'N') AS FUNDED_FLEET, 
        HOLDING_CONTRACT_STATUS AS CONTRACT_STATUS_HOLDING, 
        BUY_BACK AS BUY_BACK_AGREEMENT_FLAG, 
        LEASE_BACK AS USED_CAR_LEASE_FLAG, 
        SEATNB AS SEAT_NUMBER, 
        BODY_GROUP AS VEHICLE_BODY_HOLDING,
        DESC_VEHICLETYPE AS VEHICLE_TYPE_HOLDING, 
        DESC_MODDESCRIPTION AS MODEL_DESCRIPTION, 
        ACCOUNT_NAME AS CUSTOMER_NAME, 
        CO2_EMISSIONS, 
        COLOR AS VEHICLE_COLOR, 
        CD_DAY_START AS START_DATE,
        DESC_GENERIC_MODEL AS MODEL_HOLDING, 
        CD_PLATE AS PLATE, 
        DESC_MAKE AS MAKE_HOLDING, 
        CD_DOORS_NUMBER AS DOORS_NUMBER, 
        DESC_FUELTYPE AS FUEL_TYPE_HOLDING, 
        DESC_COMPANY, 
        DESC_COUNTRY AS COUNTRY, 
        CD_CURRENCY, 
        GEAR_BOX AS TRANSMISSION_HOLDING, 
        DATE_REFRESH, 
        RV_BASE_AMOUNT AS RV_BASE_AM_LOC, 
        UCS_RESULT, 
        DATE_SNAPSHOT AS SNAPSHOT_PERIOD, 
        DESC_COMPANY AS SUBSIDIARY_NAME, 
        (START_KM + CONTRACTUAL_KM) AS EST_TOTAL_KM,
        VAT, 
        THA_NORMALIZED_NAME, 
        'FLEET_CSV' AS SOURCE_TABLE
    FROM 
        pricing.pdm_fleet_csv
    WHERE CONCAT(CD_COMPANY, '_', CAMPAIGN) in (""" + max_campaign_list_str+ """)
    
    """
    )
)

# COMMAND ----------

# MAGIC %md ## Fleet LP

# COMMAND ----------

df_fleet_lp = spark.sql(
    """
    SELECT 
        KEY_CONTRACT, 
        CD_COMPANY AS SUBSIDIARY_CODE,
        CD_DAY_RETURN AS EST_RETURN_DATE, 
        CD_CONTRACT AS CONTRACT_NUMBER, 
        CD_CURRENCY, 
        CONTRACT_DURATION,
        CONTRACT_DURATION / 30 AS EST_TOTAL_DURATION, 
        CONTRACTUAL_KM, 
        CD_PLATE AS PLATE, 
        HOLDING_CONTRACT_TYPE AS PRODUCT_TYPE_HOLDING,
        IF(HOLDING_CONTRACT_TYPE IN ('FULL OPERATIONAL LEASING', 'FINANCIAL LEASE', 'POOL FLEET'), 'Y', 'N') AS FUNDED_FLEET, 
        HOLDING_CONTRACT_STATUS AS CONTRACT_STATUS_HOLDING, 
        BUY_BACK AS BUY_BACK_AGREEMENT_FLAG, 
        LEASE_BACK AS USED_CAR_LEASE_FLAG, 
        SEATNB AS SEAT_NUMBER, 
        INSCOPE, 
        RV_BASE_AMOUNT AS RV_BASE_AM_LOC, 
        CO2_EMISSIONS, 
        CD_DOORS_NUMBER AS DOORS_NUMBER,
        DESC_FUELTYPE AS FUEL_TYPE_HOLDING,
        GEAR_BOX AS TRANSMISSION_HOLDING,
        DESC_GENERIC_MODEL AS MODEL_HOLDING,
        DESC_VEHICLETYPE AS VEHICLE_TYPE_HOLDING,
        VEH_STATUS_CD AS VEHICLE_STATUS_LOCAL_CODE,
        DESC_MODDESCRIPTION AS MODEL_DESCRIPTION,
        DESC_MAKE AS MAKE_HOLDING,
        DESC_COUNTRY AS COUNTRY, 
        AVG_PRICE_OPTIONAL AS PRICE_OPTIONAL_AM_LOC,
        AVG_PRICE_VEHICLE AS PRICE_VEHICLE_AM_LOC,
        AVG_DISCOUNT_PRICE AS PRICE_VEHICLE_DISC_AM_LOC,
        BODY_GROUP AS VEHICLE_BODY_HOLDING,
        HOLDING_CUSTOMERPROFILE as CUSTOMER_LEGAL_TYPE,
        DATE_FROM AS SNAPSHOT_PERIOD,
        CONTRACT_STATUS_LOCAL AS CONTRACT_STATUS_LOCAL_CODE,
        CONTRACT_TYPE_LOCAL AS PRODUCT_TYPE_LOCAL_CODE,
        GEAR_BOX_LOCAL AS TRANSMISSION_LOCAL_CODE, 
        VEHICLETYPE_LOCAL AS VEHICLE_TYPE_LOCAL_CODE,
        MODEL_LOCAL AS MAKE_LOCAL_CODE,
        FUEL_LOCAL AS FUEL_TYPE_LOCAL_CODE,
        BODYGROUP_LOCAL AS VEHICLE_BODY_LOCAL_CODE,
        MIGRATED AS MIGRATED_FLAG,
        ISOVERDUE AS OVERDUE_FLAG,
        AVG_OVERDUE_DAYS AS OVERDUE_DAYS_CURRENT,
        LAST_KNOWN_MILEAGE_DATE AS LAST_KNOWN_KM_DATE,
        VEH_STATUS_DESC AS VEHICLE_STATUS_HOLDING,
        DATE_REFRESH,
        THA_NORMALIZED_NAME,
        CC,
        DIN AS DIN_HORSEPOWER,
        DESC_MAKETSECTOR AS MARKET_SECTOR_HOLDING,
        COLOR AS VEHICLE_COLOR,
        ORDER_DATE,
        CD_DAY_START AS START_DATE,
        START_KM,
        (START_KM + CONTRACTUAL_KM) AS EST_TOTAL_KM,
        BOOK_VALUE AS EST_END_BOOK_VALUE_AM_LOC,
        BOOK_VALUE_EURO AS EST_END_BOOK_VALUE_AM_EUR,
        'FLEET_LP' AS SOURCE_TABLE,
        VIN
    FROM 
        pricing.pdm_fleet_lp
    """
)

# COMMAND ----------

# MAGIC %md # Orders

# COMMAND ----------

df_orders = spark.sql(
    """
    SELECT 
        KEY_CONTRACT,
        CD_COMPANY AS SUBSIDIARY_CODE,
        CD_CONTRACT AS CONTRACT_NUMBER,
        CUSTOMER_NAME,
        CD_DAY_RETURN AS EST_RETURN_DATE,
        CD_DAY_START AS START_DATE,
        CD_CURRENCY,
        CONTRACT_DURATION,
        CONTRACT_DURATION / 30 AS EST_TOTAL_DURATION,
        AVG_PRICE_VEHICLE AS PRICE_VEHICLE_AM_LOC,
        AVG_DISCOUNT_PRICE AS PRICE_VEHICLE_DISC_AM_LOC,
        AVG_PRICE_OPTIONAL AS PRICE_OPTIONAL_AM_LOC,
        CONTRACTUAL_KM,
        ISOVERDUE AS OVERDUE_FLAG,
        HOLDING_CONTRACT_TYPE AS PRODUCT_TYPE_HOLDING,
        IF(HOLDING_CONTRACT_TYPE IN ('FULL OPERATIONAL LEASING', 'FINANCIAL LEASE', 'POOL FLEET'), 'Y', 'N') AS FUNDED_FLEET,
        HOLDING_CONTRACT_STATUS AS CONTRACT_STATUS_HOLDING,
        HOLDING_CUSTOMERPROFILE as CUSTOMER_LEGAL_TYPE,
        BODYGROUP_LOCAL AS VEHICLE_BODY_LOCAL_CODE,
        BUY_BACK AS BUY_BACK_AGREEMENT_FLAG,
        LEASE_BACK AS USED_CAR_LEASE_FLAG,
        SEATNB AS SEAT_NUMBER,
        DESC_BODYGROUP AS VEHICLE_BODY_HOLDING,
        DESC_VEHICLETYPE AS VEHICLE_TYPE_HOLDING,
        VEH_STATUS_CD AS VEHICLE_STATUS_LOCAL_CODE,
        CONTRACT_STATUS_LOCAL AS CONTRACT_STATUS_LOCAL_CODE,
        DESC_GENERIC_MODEL AS MODEL_HOLDING,
        DESC_MODDESCRIPTION AS MODEL_DESCRIPTION,
        CD_PLATE AS PLATE,
        VEHICLETYPE_LOCAL AS VEHICLE_TYPE_LOCAL_CODE,
        DESC_MAKE AS MAKE_HOLDING,
        CO2_EMISSIONS,
        CD_DOORS_NUMBER AS DOORS_NUMBER,
        DESC_FUELTYPE AS FUEL_TYPE_HOLDING,
        DESC_GEARBOX AS TRANSMISSION_HOLDING,
        DESC_COMPANY,
        DESC_COUNTRY AS COUNTRY,
        MARKET_SECTOR,
        DATE_INS,
        VAT,
        LAST_DAY(DATEADD(month, -1, DATE_INS)) AS SNAPSHOT_PERIOD,
        DESC_COMPANY AS SUBSIDIARY_NAME,
        CONTRACT_TYPE_LOCAL AS PRODUCT_TYPE_LOCAL_CODE,
        MODEL_LOCAL AS MAKE_LOCAL_CODE,
        COLOR AS VEHICLE_COLOR,
        FUEL_LOCAL AS FUEL_TYPE_LOCAL_CODE,
        MARKET_SECTOR AS MARKET_SECTOR_HOLDING,
        CC,
        GEAR_BOX_LOCAL AS TRANSMISSION_LOCAL_CODE,
        CONTRACTDATEORDERING AS ORDER_DATE,
        DIN AS DIN_HORSEPOWER,
        START_KM,
        (START_KM + CONTRACTUAL_KM) AS EST_TOTAL_KM,
        residualValueAmount AS RV_BASE_AM_LOC,
        "ORDER" AS SOURCE_TABLE,
        VIN
    FROM 
        pricing.pdm_orders_mv
    """
)

# COMMAND ----------

# MAGIC %md ### Add missing column based on final table

# COMMAND ----------

def add_missing_columns_table(df):
    
    # Find columns that are in df and not in table
    missing_columns = set(columns_ref) - set(df.columns)

    # Add missing columns with null values
    for col in missing_columns:
        df = df.select("*", F.lit(None).alias(col))

    # Ensure the columns are in the same order
    df = df.select(columns_ref)

    return df

# COMMAND ----------

df_fleet = add_missing_columns_table(df_fleet)
df_fleet_csv = add_missing_columns_table(df_fleet_csv)
df_fleet_lp = add_missing_columns_table(df_fleet_lp)
df_orders = add_missing_columns_table(df_orders)

# COMMAND ----------

# MAGIC %md # UCS

# COMMAND ----------

# MAGIC %md ## UCS CSV Aggregated

# COMMAND ----------

df_ucs_agg = spark.sql(
    """
    SELECT DISTINCT
        KEY_CONTRACT,
        CONTRACT_START_DATE AS START_DATE,
        CD_COMPANY AS SUBSIDIARY_CODE,
        CD_CONTRACT AS CONTRACT_NUMBER,
        CONTRACT_DURATION,
        CONTRACT_DURATION / 30 AS EST_TOTAL_DURATION,
        CONTRACT_RETURN_DATE as EST_RETURN_DATE,
        CONTRACT_START_DATE,
        CONTRACT_RETURN_DATE,
        CD_PLATE AS PLATE,
        CAR_SOLD_Y_N AS CAR_SOLD,
        CD_CURRENCY,
        CO2_EMISSIONS,
        CONTRACTUAL_KM,
        CUSTOMER_DISPUTES AS CUSTOMER_DISPUTES_AM_LOC,
        CUSTOMER_ID,
        DESC_MAKE AS MAKE_HOLDING,
        DESC_COUNTRY AS COUNTRY,
        DESC_FUELTYPE AS FUEL_TYPE_HOLDING,
        DESC_GEARBOX AS TRANSMISSION_HOLDING,
        DESC_GENERIC_MODEL AS MODEL_HOLDING,
        DESC_VEHICLETYPE AS VEHICLE_TYPE_HOLDING,
        VEH_STATUS_CD AS VEHICLE_STATUS_LOCAL_CODE,
        MODEL_LOCAL AS MAKE_LOCAL_CODE,
        BODYGROUP_LOCAL AS VEHICLE_BODY_LOCAL_CODE,
        GEARBOX_LOCAL AS TRANSMISSION_LOCAL_CODE,
        CC,
        DIN AS DIN_HORSEPOWER,
        START_KM,
        (START_KM + CONTRACTUAL_KM) AS EST_TOTAL_KM,
        USED_CAR_SALE_DATE AS SALE_DATE,
        DESC_MODDESCRIPTION AS MODEL_DESCRIPTION,
        EARLY_TERMINATION_INCOME AS EARLY_TERM_INCOME_AM_LOC,
        CONTRACT_STATUS_LOCAL AS CONTRACT_STATUS_LOCAL_CODE,
        VEHICLETYPE_LOCAL AS VEHICLE_TYPE_LOCAL_CODE,
        ERROR_MSG,
        EXCESS_MILEAGE_CHARGE_MAINT,
        EXCESS_MILEAGE_CHARGE AS EXCESS_MILEAGE_AM_LOC,
        EXCLUDE_FOR_ANALYSIS,
        GROUP_PROCCEDS AS PROCEEDS_FROM_SALE_AM_LOC,
        HOLDING_CONTRACT_TYPE AS PRODUCT_TYPE_HOLDING,
        IF(HOLDING_CONTRACT_TYPE IN ('FULL OPERATIONAL LEASING', 'FINANCIAL LEASE', 'POOL FLEET'), 'Y', 'N') AS FUNDED_FLEET,
        HOLDING_CONTRACT_STATUS AS CONTRACT_STATUS_HOLDING,
        HOLDING_CUSTOMERPROFILE as CUSTOMER_LEGAL_TYPE,
        INCLUDED_IN_PALBS,
        DESC_BODYGROUP AS VEHICLE_BODY_HOLDING,
        MARKET_SECTOR AS MARKET_SECTOR_HOLDING,
        (REFURBISHMENT_COST + USED_CAR_SALE_AMOUNT + RECONDITIONING_INCOME_BILLED) AS TOTAL_SALE_AM_LOC,
        INSCOPE,
        BUY_BACK AS BUY_BACK_AGREEMENT_FLAG,
        LEASE_BACK AS USED_CAR_LEASE_FLAG,
        LP_OPTIONS,
        MARKET_SECTOR,
        MARKETING_OTHER_SALE_COSTS AS MARKETING_OTHER_COSTS_AM_LOC,
        CONTRACT_TYPE_LOCAL AS PRODUCT_TYPE_LOCAL_CODE,
        NET_BOOK_VALUE AS ACT_END_BOOK_VALUE_AM_LOC,
        MODEL_AGE,
        MONTH_REPORTED_TO_PALBS,
        NET_BOOK_VALUE,
        NET_GUIDE_BOOK_VALUE,
        NEW_BODYGROUP,
        NEW_GEARBOX,
        PER_UCS,
        UCS_PERFORMANCE,
        AVG_PRICE_OPTIONAL AS PRICE_OPTIONAL_AM_LOC,
        AVG_PRICE_VEHICLE AS PRICE_VEHICLE_AM_LOC,
        TOTAL_SALE_PRICE AS PRICE_TOTAL_AM_LOC,
        AVG_DISCOUNT_PRICE AS PRICE_VEHICLE_DISC_AM_LOC,
        PROFIT_SHARE AS PROFIT_SHARING,
        REAL_RETURN_DATE,
        REAL_RETURN_KM,
        REFURBISHMENT_COST,
        REGISTRATION_DATE,
        RESIDUALVALUEWITHOUTVAT,
        ROUND_AGE_SALES,
        RV_BASE_AMOUNT AS RV_BASE_AM_LOC,
        RV_UCS_PCT AS UCS_PCT,
        RV_UCS_PCT2,
        SALE_CHANNEL AS REMARKETING_CHANNEL_HOLDING,
        SALE_CHANNEL_LOCAL AS REMARKETING_CHANNEL_LOCAL_CODE,
        FUEL_LOCAL AS FUEL_TYPE_LOCAL_CODE,
        CD_DOORS_NUMBER AS DOORS_NUMBER,
        COLOR AS VEHICLE_COLOR,
        SEATNB AS SEAT_NUMBER,
        SOURCE_SYS,
        STOCK_DAYS,
        TERMINATION_REASON,
        UCS_RESULT as UCS_RESULT_AM_LOC,
        UPLIFT_AMOUNT,
        UPLIFT_PALBS_ACCOUNT,
        UPLIFT,
        USED_CAR_SALE_AMOUNT,
        DATEDIFF(CURRENT_DATE, USED_CAR_SALE_DATE) AS ACT_TOTAL_DURATION_AT_SALE,
        VAT,
        VEH_STATUS,
        VEHICLE_INVENTORY_PROVISION,
        VEHICLETYPE_LOCAL,
        VEHICLE_TYPE_PALBS,
        ACCOUNTNAME_NVA,
        AGE_VEHICLE,
        TERMINATION_REASON AS ENDED_REASON_HOLDING,
        DATE_REFRESH,
        MONTH_REPORTED_TO_PALBS AS SNAPSHOT_PERIOD,
        RECONDITIONING_INCOME_BILLED AS RECONDITIONING_INCOME_AM_LOC,
        DESC_COMPANY AS SUBSIDIARY_NAME,
        REFURBISHMENT_COST AS REFURBISHMENT_COST_AM_LOC,
        DELIVERY_COLLECTION_COSTS AS DELIVERY_COLLECTION_COSTS_AM_LOC,
        'UCS_AGG' AS SOURCE_TABLE
    FROM 
        pricing.pdm_ucs_agg 
    """
)

# COMMAND ----------

# MAGIC %md ## UCS LP Aggregated

# COMMAND ----------

df_ucs_lp_agg = spark.sql(
    """
    SELECT 
        KEY_CONTRACT,
        CONTRACT_NUMBER,
        ACCOUNT_NAME as CUSTOMER_NAME,
        ACCOUNTNAME_NVA,
        BODY_GROUP as VEHICLE_BODY_HOLDING,
        BUY_BACK as BUY_BACK_AGREEMENT_FLAG,
        CAR_SOLD_Y_N as CAR_SOLD,
        CD_COMPANY as SUBSIDIARY_CODE,
        CONTRACT_DURATION,
        CONTRACT_DURATION / 30 as EST_TOTAL_DURATION,
        CONTRACT_RETURN_DATE as EST_RETURN_DATE,
        CONTRACTUAL_KM,
        DOORS_NUMBER,
        CO2_EMISSIONS,
        CURRENCY as CD_CURRENCY,
        FUEL_TYPE as FUEL_TYPE_HOLDING,
        GEARBOX as TRANSMISSION_HOLDING,
        GENERIC_MODEL as MODEL_HOLDING,
        HOLDING_CONTRACT_STATUS as CONTRACT_STATUS_HOLDING,
        HOLDING_CUSTOMERPROFILE as CUSTOMER_LEGAL_TYPE,
        HOLDING_CONTRACT_TYPE as PRODUCT_TYPE_HOLDING,
        IF(HOLDING_CONTRACT_TYPE IN ('FULL OPERATIONAL LEASING','FINANCIAL LEASE','POOL FLEET'), 'Y', 'N') AS FUNDED_FLEET,
        INSCOPE,
        LEASE_BACK as USED_CAR_LEASE_FLAG,
        LIST_PRICE_ACCESSORIES as PRICE_ACCESSORIES_AM_LOC,
        MAKE as MAKE_HOLDING,
        MARKET_SECTOR,
        MODEL_AGE,
        PLATE,
        SEATNB as SEAT_NUMBER,
        DETAILED_MODEL as MODEL_DESCRIPTION,
        THA_NORMALIZED_NAME,
        TOTAL_SALE_PRICE as PRICE_TOTAL_AM_LOC,
        UPLIFT_PALBS_ACCOUNT,
        USED_CAR_SALE_AMOUNT,
        VAT,
        LP_OPTIONS,
        PROFIT_SHARE as PROFIT_SHARING,
        REAL_RETURN_DATE,
        REAL_RETURN_KM,
        REFURBISHMENT_COST,
        REGISTRATION_DATE,
        RESIDUALVALUEWITHOUTVAT,
        ROUND_AGE_SALES,
        RV_BASE_AMOUNT as RV_BASE_AM_LOC,
        RV_UCS_PCT as UCS_PCT,
        RV_UCS_PCT2,
        SALE_CHANNEL as REMARKETING_CHANNEL_HOLDING,
        SALE_CHANNEL_LOCAL as REMARKETING_CHANNEL_LOCAL_CODE,
        SOURCE_SYS,
        STOCK_DAYS,
        TERMINATION_REASON,
        UCS_RESULT as UCS_RESULT_AM_LOC,
        UPLIFT_AMOUNT,
        USED_CAR_SALE_DATE as SALE_DATE,
        VEH_STATUS,
        VEHICLE_INVENTORY_PROVISION,
        VEHICLE_TYPE_PALBS,
        AGE_VEHICLE,
        MONTH_REPORTED_TO_PALBS as SNAPSHOT_PERIOD,
        COUNTRY,
        VEHICLE_TYPE as VEHICLE_TYPE_HOLDING,
        VEH_STATUS_CD as VEHICLE_STATUS_LOCAL_CODE,
        CONTRACT_STATUS_LOCAL as CONTRACT_STATUS_LOCAL_CODE,
        CONTRACT_START_DATE as START_DATE,
        CONTRACT_TYPE_LOCAL as PRODUCT_TYPE_LOCAL_CODE,
        GEARBOX_LOCAL as TRANSMISSION_LOCAL_CODE,
        VEHICLETYPE_LOCAL as VEHICLE_TYPE_LOCAL_CODE,
        MODEL_LOCAL as MAKE_LOCAL_CODE,
        FUEL_LOCAL as FUEL_TYPE_LOCAL_CODE,
        MARKET_SECTOR as MARKET_SECTOR_HOLDING,
        BODYGROUP_LOCAL as VEHICLE_BODY_LOCAL_CODE,
        DIN_HORSEPOWER,
        CUBIC_ENGINE_CAPACITY as CC,
        COLOR as VEHICLE_COLOR,
        START_KM,
        (START_KM + CONTRACTUAL_KM) as EST_TOTAL_KM,
        0 as TECHNICAL_RV_AM_LOC,
        0 as LOADING_RV_AM_LOC,
        NET_BOOK_VALUE as ACT_END_BOOK_VALUE_AM_LOC,
        TERMINATION_REASON as ENDED_REASON_HOLDING,
        "UCS_AGG_LP" as SOURCE_TABLE,
        VIN
    FROM 
        pricing.pdm_ucs_lp_agg
    """
)

# To be included in 2024 11
# OPEN_CALCULATION,
# RV_LOADING_AM as LOADING_RV_AM_LOC,

# COMMAND ----------

df_ucs_lp_agg = add_missing_columns_table(df_ucs_lp_agg)
df_ucs_agg = add_missing_columns_table(df_ucs_agg)

# COMMAND ----------

# MAGIC %md # Stock

# COMMAND ----------

df_stock = spark.sql("""
    SELECT 
        c.contractCode AS CONTRACT_NUMBER, 
        CONCAT(c.subsidiaryCode, '_', c.contractCode) AS KEY_CONTRACT,
        c.subsidiaryCode AS SUBSIDIARY_CODE,
        c.vehicleCode AS VEHICLECODE,
        c.contractDateForecastTermination AS EST_RETURN_DATE,
        'MASKED' AS CUSTOMER_NAME,
        v.vehCharBodyCode AS VEHICLE_BODY_LOCAL_CODE,
        IF(ISNULL(c.contractMileageReestimated) OR c.contractMileageReestimated <= 0, 
           c.contractMileageInitialyEstimated, c.contractMileageReestimated) AS CONTRACTUAL_KM,
        IF(ISNULL(c.contractDurationReestimated) OR c.contractDurationReestimated <= 0, 
           c.contractDurationInitialyEstimated, c.contractDurationReestimated) AS CONTRACT_DURATION,
        (IF(ISNULL(c.contractDurationReestimated) OR c.contractDurationReestimated <= 0, 
           c.contractDurationInitialyEstimated, c.contractDurationReestimated)) / 30 AS EST_TOTAL_DURATION,
        'HOLDING_CONTRACT_TYPE' AS PRODUCT_TYPE_HOLDING,
        v.vehPriceOLWOVATAmount AS PRICE_OPTIONAL_AM_LOC,
        'HOLDING_CONTRACT_STATUS' AS CONTRACT_STATUS_HOLDING,
        'HOLDING_CUSTOMERPROFILE' AS CUSTOMER_LEGAL_TYPE,
        CAST(c.contractIsBuyBack AS STRING) AS BUY_BACK_AGREEMENT_FLAG,
        CAST(v.vehCharIsBoughtNew AS STRING) AS USED_CAR_LEASE_FLAG,
        v.vehCharSeatNb AS SEAT_NUMBER,
        v.vehPriceWoOLWoVATAmount AS PRICE_VEHICLE_AM_LOC,
        v.vehPriceWONWoVATAmount AS PRICE_VEHICLE_DISC_AM_LOC,
        c.contractStatusCode AS CONTRACT_STATUS_LOCAL_CODE,
        v.vehCharTransmissionCode AS TRANSMISSION_LOCAL_CODE,
        v.vehCharPowerCC AS CC,
        c.contractMileageStart AS START_KM,
        'BODY_GROUP' AS VEHICLE_BODY_HOLDING,
        'DESC_VEHICLETYPE' AS VEHICLE_TYPE_HOLDING,
        v.vehCharTypeCode AS VEHICLE_TYPE_LOCAL_CODE,
        v.vehCharModel AS MODEL_DESCRIPTION,
        v.vehCharCO2Emission as CO2_EMISSIONS,
        v.vehRegPlateNumber as PLATE,
        'MAKE_HOLDING' AS MAKE_HOLDING,
        v.vehCharDoorNb AS DOORS_NUMBER,
        'FUEL_TYPE_HOLDING' AS FUEL_TYPE_HOLDING,
        v.vehPriceWOLCurrency as CD_CURRENCY,
        'TRANSMISSION_HOLDING' AS TRANSMISSION_HOLDING,
        v.vehicleIdentificationNumber AS VIN,
        v.vehPriceONWoVATAmount as PRICE_DISC_AM_LOC_veh,
        v.vehCharModelVersion as MODEL_VERSION,
        v.vehCharModelYear as MODEL_YEAR,
        v.vehCharWeightNet as TARE_WEIGHT,
        v.vehicleIdentificationNumber as VIN_veh,
        v.vehCharConsumption as FUEL_CONSUMPTION,
        v.vehCharPowerKW as ELECTRICITY_CONSUMPTION,
        v.vehCharGearNb as GEARS_NUMBER,
        v.vehCharWeightMax as GROSS_WEIGHT,
        c.contractDateTermination as ACT_RETURN_DATE,
        c.contractDurationExtension as EARLY_OVERDUE_DAYS_ACTUAL,
        c.contractMileageReestimated as CONTRACTUAL_KM_UPDATED,
        c.contractDurationInitialyEstimated/30 as INIT_DURATION,
        c.contractDurationInitialyEstimated as CONTRACT_DURATION_INITIAL,
        c.contractDurationReestimated as CONTRACT_DURATION_UPDATED, 
        DATEDIFF(CURRENT_DATE , c.contractDateTermination) as ACT_TOTAL_DURATION_AT_RETURN,
        c.dataActionDate as DATE_REFRESH,
        c.dataActionDate as DATE_INS,
        c.insertDate as SNAPSHOT_PERIOD,
        v.vehCharStatusCode as VEHICLE_STATUS_LOCAL_CODE,
        v.vehCharColorExtCode as VEHICLE_COLOR,
        c.productFamilyCode AS PRODUCT_TYPE_LOCAL_CODE,
        vehPriceWOLVATRate as VAT_PCT,
        v.vehCharMakeCode as MAKE_LOCAL_CODE,
        v.vehCharEnergyCode as FUEL_TYPE_LOCAL_CODE,
        v.vehCharPowerDIN as DIN_HORSEPOWER,
        c.contractDateOrdering as ORDERING_DATE,
        c.contractRealStartDate as CD_DAY_START,
        (c.contractMileageStart + IF(ISNULL(contractMileageReestimated) or contractMileageReestimated <=  0, contractMileageInitialyEstimated, contractMileageReestimated) ) as EST_TOTAL_KM,
        'STOCK' AS SOURCE_TABLE
    FROM aldbidelta.dw_contract c 
    LEFT JOIN aldbidelta.dw_vehicle v 
    ON c.subsidiaryCode = v.subsidiaryCode AND c.vehicleCode = v.vehicleCode
    WHERE COALESCE(c.contractDateTermination, '2019') <= 2020
""")

# COMMAND ----------

# MAGIC %md # Unions 

# COMMAND ----------

df_fleet_2 = df_fleet.union(df_fleet_csv)
df_fleet_3 = df_fleet_2.union(df_fleet_lp)
df_fleet_orders = df_fleet_3.union(df_orders)
df_fleet_ucs = df_fleet_orders.union(df_ucs_agg)
df_fleet_ucs_2 = df_fleet_ucs.union(df_ucs_lp_agg)

df = df_fleet_ucs_2

# COMMAND ----------

# MAGIC %md ### Stock calculation

# COMMAND ----------

df_diff_stock = df_stock.join(df, df_stock.KEY_CONTRACT == df.KEY_CONTRACT, "left_anti")
print(df_diff_stock.count())

# COMMAND ----------

df_diff_stock = add_missing_columns_table(df_diff_stock)
df = df.union(df_diff_stock)

# COMMAND ----------

# MAGIC %md ## Adjust schema: conver all the column in datafram to the column types in table

# COMMAND ----------

schema = spark.table("pricing.pdm_contract_full").schema

select_exprs = [col(field.name).cast(field.dataType).alias(field.name) for field in schema]
df = df.select(*select_exprs)

# COMMAND ----------

# MAGIC %md # Calculated columns

# COMMAND ----------

# MAGIC %md #### Contract 

# COMMAND ----------

df_ctr = spark.sql("""select subsidiaryCode, 
                   contractCode, 
                   vehicleCode as VEHICLECODE,
                   contractDateTermination as ACT_RETURN_DATE,
                   contractDurationExtension as EARLY_OVERDUE_DAYS_ACTUAL,
                   contractMileageInitialyEstimated as CONTRACTUAL_KM_INITIAL,
                   contractMileageReestimated as CONTRACTUAL_KM_UPDATED,
                   contractDurationInitialyEstimated/30 as INIT_DURATION,
                   contractDurationInitialyEstimated as CONTRACT_DURATION_INITIAL,
                   contractDurationReestimated as CONTRACT_DURATION_UPDATED, 
                   DATEDIFF(CURRENT_DATE , contractDateTermination) as ACT_TOTAL_DURATION_AT_RETURN
                   from aldbidelta.dw_contract""")

# COMMAND ----------

df = df.drop(
    "VEHICLECODE",
    "ACT_RETURN_DATE",
    "EARLY_OVERDUE_DAYS_ACTUAL",
    "CONTRACTUAL_KM_INITIAL",
    "CONTRACTUAL_KM_UPDATED",
    "INIT_DURATION",
    "CONTRACT_DURATION_INITIAL",
    "CONTRACT_DURATION_UPDATED",
    "ACT_TOTAL_DURATION_AT_RETURN"
)

df = df.join(
    df_ctr,
    (df.SUBSIDIARY_CODE == df_ctr.subsidiaryCode) & 
    (df.CONTRACT_NUMBER == df_ctr.contractCode),
    'left'
).select(
    df["*"],
    df_ctr["VEHICLECODE"],
    df_ctr["ACT_RETURN_DATE"],
    df_ctr["EARLY_OVERDUE_DAYS_ACTUAL"],
    df_ctr["CONTRACTUAL_KM_INITIAL"],
    df_ctr["CONTRACTUAL_KM_UPDATED"],
    df_ctr["INIT_DURATION"],
    df_ctr["CONTRACT_DURATION_INITIAL"],
    df_ctr["CONTRACT_DURATION_UPDATED"],
    df_ctr["ACT_TOTAL_DURATION_AT_RETURN"]
)

# COMMAND ----------

# MAGIC %md ### Vehicle

# COMMAND ----------

df_veh = spark.sql("""
          SELECT subsidiaryCode,
          vehicleCode as VEHICLECODE,
          vehPriceONWoVATAmount as PRICE_DISC_AM_LOC_veh,
          vehCharModelVersion as MODEL_VERSION,
          vehCharModelYear as MODEL_YEAR,
          vehCharWeightNet as TARE_WEIGHT,
          vehicleIdentificationNumber as VIN_veh,
          vehCharConsumption as FUEL_CONSUMPTION,
          vehCharPowerKW as ELECTRICITY_CONSUMPTION,
          vehCharGearNb as GEARS_NUMBER,
          vehCharWeightMax as GROSS_WEIGHT,
          vehCharJatoCode 
          from  """+BBDDALDBI+""".dw_vehicle 
          """
          )

# COMMAND ----------

df = df.drop(
    "MODEL_VERSION",
    "MODEL_YEAR",
    "TARE_WEIGHT",
    "FUEL_CONSUMPTION",
    "ELECTRICITY_CONSUMPTION",
    "GEARS_NUMBER",
    "GROSS_WEIGHT"
)

# COMMAND ----------



df = df.join(
    df_veh,
    (df.SUBSIDIARY_CODE == df_veh.subsidiaryCode) & 
    (df.VEHICLECODE == df_veh.VEHICLECODE),
    'left'
).select(
    df["*"],
    df_veh["MODEL_VERSION"],
    df_veh["MODEL_YEAR"],
    df_veh["TARE_WEIGHT"],
    df_veh["VIN_veh"],
    df_veh["FUEL_CONSUMPTION"],
    df_veh["ELECTRICITY_CONSUMPTION"],
    df_veh["GEARS_NUMBER"],
    df_veh["GROSS_WEIGHT"],
    df_veh["vehCharJatoCode"]
)

# COMMAND ----------

columns_ref_tmp = [elem for elem in columns_ref if elem != 'VIN']
columns_ref_tmp.append('vehCharJatoCode')

df = df.select(*columns_ref_tmp,  F.coalesce(df["VIN"], df["VIN_veh"]).alias("VIN") ).drop("VIN_veh")

# COMMAND ----------

# MAGIC %md #### Battery related data from TCO staging

# COMMAND ----------

df_battery = spark.sql("""SELECT 
        dataHeader_subsidiaryCode,
        jatocode,
        avg(vehiclebatterycapacity) as BATTERY_RANGE,
        avg(vehicleelectricrange)  AS BATTERY_CAPACITY_NOMINAL
    FROM stagingtcocal.dw_tcocalculatorvehiclehist 
    WHERE dataHeader_current = true
    group by dataHeader_subsidiaryCode, jatocode
""")

# COMMAND ----------

df = df.drop("BATTERY_RANGE").drop("BATTERY_CAPACITY_NOMINAL")
df = df.join( df_battery,(df.SUBSIDIARY_CODE == df_battery.dataHeader_subsidiaryCode) & (df.vehCharJatoCode == df_battery.jatocode), "left").select(df["*"],df_battery["BATTERY_RANGE"],df_battery["BATTERY_CAPACITY_NOMINAL"])
df = df.drop("vehCharJatoCode")

# COMMAND ----------

# MAGIC %md ## Exchange rate

# COMMAND ----------

try:
    df_ex_rate = spark.sql("""
        SELECT t.CURRENCYCODE, t.AVGRATE as EXCHANGE_RATE
        FROM sql.cf_fxrate_t t
        JOIN (
            SELECT CURRENCYCODE, MAX(VALIDITYDATE) AS MAX_VALIDITYDATE
            FROM sql.cf_fxrate_t
            GROUP BY CURRENCYCODE
        ) max_t
        ON t.CURRENCYCODE = max_t.CURRENCYCODE AND t.VALIDITYDATE = max_t.MAX_VALIDITYDATE
        """)

    #First drop dummy column
    df = df.drop("EXCHANGE_RATE")

    df = df.join( df_ex_rate, df.CURRENCY_ISO == df_ex_rate.CURRENCYCODE, "left").select(df["*"],df_ex_rate["EXCHANGE_RATE"])
except:
    print("Exchange rate table not available")

# COMMAND ----------

df = df.withColumn("EXCHANGE_RATE", when(col("EXCHANGE_RATE").isNull() | (col("EXCHANGE_RATE") == 0), 1).otherwise(col("EXCHANGE_RATE")))

# COMMAND ----------

# MAGIC %md #### Euro to local conversion

# COMMAND ----------

#
col_to_convert = ['PRICE_VEHICLE_AM_LOC',
                  'PRICE_VEHICLE_DISC_AM_LOC', 
                  'PRICE_OPTIONAL_AM_LOC', 
                  'PRICE_TOTAL_AM_LOC', 
                  'PRICE_ACCESSORIES_AM_LOC', 
                  'PRICE_OPTIONAL_DISC_AM_LOC', 
                  'INVESTMENT_AM_LOC', 
                  'ACT_BOOK_VALUE_LOC', 
                  'MONTHLY_DEPRECIATION_AM_LOC', 
                  'TECHNICAL_RV_AM_LOC', 
                  'LOADING_RV_AM_LOC', 
                  'RV_BASE_AM_LOC', 
                  'PROCEEDS_FROM_SALE_AM_LOC', 
                  'RECONDITIONING_INCOME_AM_LOC', 
                  'REFURBISHMENT_COST_AM_LOC', 
                  'EARLY_TERM_INCOME_AM_LOC', 
                  'TOTAL_SALE_AM_LOC', 
                  'EXCESS_MILEAGE_AM_LOC', 
                  'DELIVERY_COLLECTION_COSTS_AM_LOC', 
                  'MARKETING_OTHER_COSTS_AM_LOC', 
                  'CUSTOMER_DISPUTES_AM_LOC', 
                  'ACT_END_BOOK_VALUE_AM_LOC']    

for c in col_to_convert:
    df = df.drop(c[:-4] +"_EUR")
    df = df.select("*", (F.when(col("CURRENCY_ISO") != "EUR", F.col(c) / F.col("EXCHANGE_RATE")).otherwise(F.col(c))).alias(c[:-4] +"_EUR"))



# COMMAND ----------

# MAGIC %md ## Subsidary 

# COMMAND ----------

df_sub = spark.sql("""
    SELECT 
        subsidiaryCode as SUBSIDIARY_CODE, 
        upper(COMPANYNAME) as SUBSIDIARY_NAME_SUB,
        HUBCODE as HUB_NAME, 
        BOSSYSTEM as BACK_OFFICE_SYSTEM,
        COUNTRYCODE as COUNTRY_CODE, 
        ISO_COUNTRY_CODE as COUNTRY_CODE_ISO 
    FROM sql.CF_SUBSIDIARIES_T
""")

df = df.drop("HUB_NAME").drop("BACK_OFFICE_SYSTEM").drop("COUNTRY_CODE").drop("COUNTRY_CODE_ISO")

df = df.join(df_sub, on=['SUBSIDIARY_CODE'], how='left').select(
    df["*"],
    df_sub["HUB_NAME"],
    df_sub["BACK_OFFICE_SYSTEM"],
    df_sub["COUNTRY_CODE"],
    df_sub["COUNTRY_CODE_ISO"],
    df_sub["SUBSIDIARY_NAME_SUB"]
)


# COMMAND ----------

columns_ref_tmp = [elem for elem in columns_ref if elem != 'SUBSIDIARY_NAME']

df = df.select(*columns_ref_tmp,  F.coalesce(df["SUBSIDIARY_NAME"], df["SUBSIDIARY_NAME_SUB"]).alias("SUBSIDIARY_NAME") ).drop("SUBSIDIARY_NAME_SUB")


# COMMAND ----------

# MAGIC %md ## Vehicle Delivery

# COMMAND ----------

df_veh_del = spark.sql("""
    SELECT subsidiaryCode,
           vehicleDelivery_contractCode,
           vehicleDelivery_vehicleCode,                       
           avg(vehicleDelivery_vehicleMileageCurrent) as ACT_TOTAL_KM
    FROM """ + BBDDALDBI + """.dw_vehicledelivery
    GROUP BY subsidiaryCode,
           vehicleDelivery_contractCode,
           vehicleDelivery_vehicleCode
""")

df = df.drop("ACT_TOTAL_KM")

df = df.join(
    df_veh_del,
    ((df.SUBSIDIARY_CODE == df_veh_del.subsidiaryCode) & 
    (df.CONTRACT_NUMBER == df_veh_del.vehicleDelivery_contractCode) & 
    (df.VEHICLECODE == df_veh_del.vehicleDelivery_vehicleCode)),
    how='left'
).select(df["*"], df_veh_del["ACT_TOTAL_KM"])


# COMMAND ----------

# MAGIC %md ## Acquisition cost

# COMMAND ----------

df_ac = spark.sql("""
    SELECT subsidiaryCode, 
           contractCode, 
           acquisitionCostAmount as INVESTMENT_AM_LOC, 
           netBookValueAmount as ACT_BOOK_VALUE_LOC
    FROM (
        SELECT subsidiaryCode, 
               contractCode, 
               acquisitionCostAmount,
               netBookValueAmount, 
               cutOffDate,
               ROW_NUMBER() OVER (PARTITION BY subsidiaryCode, contractCode ORDER BY cutOffDate DESC) as row_num
        FROM lillycarbon.leaseaccountingcalculated
    ) 
    WHERE row_num = 1
""")

df = df.drop("INVESTMENT_AM_LOC").drop("ACT_BOOK_VALUE_LOC")

df = df.join(
    df_ac,
    (df.SUBSIDIARY_CODE == df_ac.subsidiaryCode) & 
    (df.CONTRACT_NUMBER == df_ac.contractCode),
    how='left'
).select(df["*"], df_ac["INVESTMENT_AM_LOC"], df_ac["ACT_BOOK_VALUE_LOC"])      

# COMMAND ----------

# MAGIC %md ## Lease Operation depreciation

# COMMAND ----------

df_deprec = spark.sql("""
    SELECT subsidiaryCode, 
           contractCode, 
           monthlyDepreciationAmount as MONTHLY_DEPRECIATION_AM_LOC
           
    FROM (
        SELECT subsidiaryCode, 
               contractCode, 
               monthlyDepreciationAmount, 
               cutOffDate,
               ROW_NUMBER() OVER (PARTITION BY subsidiaryCode, contractCode ORDER BY cutOffDate DESC) as row_num
        FROM lillycarbon.leaseoperation_live
    ) 
    WHERE row_num = 1
""")

df = df.drop("MONTHLY_DEPRECIATION_AM_LOC")

df = df.join(
    df_deprec,
    (df.SUBSIDIARY_CODE == df_deprec.subsidiaryCode) & 
    (df.CONTRACT_NUMBER == df_deprec.contractCode),
    how='left'
).select(df["*"], df_deprec["MONTHLY_DEPRECIATION_AM_LOC"])       

# COMMAND ----------

# MAGIC %md ### Calculate fields when null

# COMMAND ----------

columns_ref_tmp = [elem for elem in columns_ref if elem not in ['OVERDUE_FLAG']]

df = df.select(*columns_ref_tmp,  F.coalesce(df["OVERDUE_FLAG"],  F.when(df["EST_RETURN_DATE"] > F.current_date(), 'Y').otherwise('N')).alias("OVERDUE_FLAG") )

# COMMAND ----------

columns_ref_tmp = [elem for elem in columns_ref if elem not in ['OVERDUE_DAYS_CURRENT']]

df = df.select(*columns_ref_tmp,  F.coalesce(df["OVERDUE_DAYS_CURRENT"],  F.when(F.datediff(F.current_date(), df["EST_RETURN_DATE"]) > 0, F.datediff(F.current_date(), df["EST_RETURN_DATE"])).otherwise(0)).alias("OVERDUE_DAYS_CURRENT") )

# COMMAND ----------

# MAGIC %md ### Conversion to the final table data type

# COMMAND ----------

# Computation time complete table = 9 min
schema = spark.table('pricing.pdm_contract_full').schema

field_list = ['BATTERY_RANGE'] # Adjust only for subset of fields to speed up process
# field_list = columns_ref

for field in schema:
    if(field.name in field_list):
        print(field)
        df = df.select(*[df[field.name].cast(field.dataType).alias(field.name) for field in schema])


# COMMAND ----------

# MAGIC %md ## Delta table storage

# COMMAND ----------

df = df.cache()
df.repartition(100).write.format('delta').option("mergeSchema", "true").mode("overwrite").saveAsTable('pricing.pdm_contract_full')

# COMMAND ----------

# MAGIC %md # Masking before SQL

# COMMAND ----------

dbutils.notebook.run("/Pricing/PDM_Masking", 8000, {"table": "PDM_CONTRACT_FULL", "schema": "pricing"}) 

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC
