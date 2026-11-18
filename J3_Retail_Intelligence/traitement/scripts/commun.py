"""Parametres, schemas et fonctions de lecture communs aux traitements UC-06 (specification TPCx-BB v1.7.0)."""
import time
from pyspark.sql import SparkSession, functions as F

# --- Parametres : chemins d'entree et de sortie (HDFS, Data Lake du Jour 2) ---
RACINE = "hdfs://namenode:8020/retail-lake"
RAW = RACINE + "/raw/tpcxbb"
SILVER = RACINE + "/silver"
GOLD = RACINE + "/gold"
QUALITY = RACINE + "/quality"
TEST = RACINE + "/quality/test_ecriture"      # repertoire reserve aux essais d'ecriture

SCHEMAS = {
    "store_sales": [("ss_sold_date_sk", "bigint"), ("ss_sold_time_sk", "bigint"), ("ss_item_sk", "bigint"),
        ("ss_customer_sk", "bigint"), ("ss_cdemo_sk", "bigint"), ("ss_hdemo_sk", "bigint"), ("ss_addr_sk", "bigint"),
        ("ss_store_sk", "bigint"), ("ss_promo_sk", "bigint"), ("ss_ticket_number", "bigint"), ("ss_quantity", "int"),
        ("ss_wholesale_cost", "decimal(7,2)"), ("ss_list_price", "decimal(7,2)"), ("ss_sales_price", "decimal(7,2)"),
        ("ss_ext_discount_amt", "decimal(7,2)"), ("ss_ext_sales_price", "decimal(7,2)"),
        ("ss_ext_wholesale_cost", "decimal(7,2)"), ("ss_ext_list_price", "decimal(7,2)"), ("ss_ext_tax", "decimal(7,2)"),
        ("ss_coupon_amt", "decimal(7,2)"), ("ss_net_paid", "decimal(7,2)"), ("ss_net_paid_inc_tax", "decimal(7,2)"),
        ("ss_net_profit", "decimal(7,2)")],
    "store_returns": [("sr_returned_date_sk", "bigint"), ("sr_return_time_sk", "bigint"), ("sr_item_sk", "bigint"),
        ("sr_customer_sk", "bigint"), ("sr_cdemo_sk", "bigint"), ("sr_hdemo_sk", "bigint"), ("sr_addr_sk", "bigint"),
        ("sr_store_sk", "bigint"), ("sr_reason_sk", "bigint"), ("sr_ticket_number", "bigint"),
        ("sr_return_quantity", "int"), ("sr_return_amt", "decimal(7,2)"), ("sr_return_tax", "decimal(7,2)"),
        ("sr_return_amt_inc_tax", "decimal(7,2)"), ("sr_fee", "decimal(7,2)"), ("sr_return_ship_cost", "decimal(7,2)"),
        ("sr_refunded_cash", "decimal(7,2)"), ("sr_reversed_charge", "decimal(7,2)"), ("sr_store_credit", "decimal(7,2)"),
        ("sr_net_loss", "decimal(7,2)")],
    "item": [("i_item_sk", "bigint"), ("i_item_id", "string"), ("i_rec_start_date", "string"),
        ("i_rec_end_date", "string"), ("i_item_desc", "string"), ("i_current_price", "decimal(7,2)"),
        ("i_wholesale_cost", "decimal(7,2)"), ("i_brand_id", "int"), ("i_brand", "string"), ("i_class_id", "int"),
        ("i_class", "string"), ("i_category_id", "int"), ("i_category", "string"), ("i_manufact_id", "int"),
        ("i_manufact", "string"), ("i_size", "string"), ("i_formulation", "string"), ("i_color", "string"),
        ("i_units", "string"), ("i_container", "string"), ("i_manager_id", "int"), ("i_product_name", "string")],
    "store": [("s_store_sk", "bigint"), ("s_store_id", "string"), ("s_rec_start_date", "string"),
        ("s_rec_end_date", "string"), ("s_closed_date_sk", "bigint"), ("s_store_name", "string"),
        ("s_number_employees", "int"), ("s_floor_space", "int"), ("s_hours", "string"), ("s_manager", "string"),
        ("s_market_id", "int"), ("s_geography_class", "string"), ("s_market_desc", "string"),
        ("s_market_manager", "string"), ("s_division_id", "int"), ("s_division_name", "string"),
        ("s_company_id", "int"), ("s_company_name", "string"), ("s_street_number", "string"),
        ("s_street_name", "string"), ("s_street_type", "string"), ("s_suite_number", "string"), ("s_city", "string"),
        ("s_county", "string"), ("s_state", "string"), ("s_zip", "string"), ("s_country", "string"),
        ("s_gmt_offset", "decimal(5,2)"), ("s_tax_precentage", "decimal(5,2)")],
    "reason": [("r_reason_sk", "bigint"), ("r_reason_id", "string"), ("r_reason_desc", "string")],
    "date_dim": [("d_date_sk", "bigint"), ("d_date_id", "string"), ("d_date", "string"), ("d_month_seq", "int"),
        ("d_week_seq", "int"), ("d_quarter_seq", "int"), ("d_year", "int"), ("d_dow", "int"), ("d_moy", "int"),
        ("d_dom", "int"), ("d_qoy", "int"), ("d_fy_year", "int"), ("d_fy_quarter_seq", "int"),
        ("d_fy_week_seq", "int"), ("d_day_name", "string"), ("d_quarter_name", "string"), ("d_holiday", "string"),
        ("d_weekend", "string"), ("d_following_holiday", "string"), ("d_first_dom", "int"), ("d_last_dom", "int"),
        ("d_same_day_ly", "int"), ("d_same_day_lq", "int"), ("d_current_day", "string"), ("d_current_week", "string"),
        ("d_current_month", "string"), ("d_current_quarter", "string"), ("d_current_year", "string")],
}
# Cle candidate de chaque source (verifiee a l'etape 3) ; les tables de faits n'ont pas de cle simple.
CLES = {"item": ["i_item_sk"], "store": ["s_store_sk"], "reason": ["r_reason_sk"], "date_dim": ["d_date_sk"],
        "store_sales": ["ss_ticket_number", "ss_item_sk"], "store_returns": ["sr_ticket_number", "sr_item_sk"]}
# Tables disposant d'un fichier de rafraichissement dans la zone Raw
AVEC_REFRESH = {"store_sales", "store_returns", "item"}


def heure():
    return time.strftime("%d/%m/%Y %H:%M:%S")


def titre(texte):
    print("\n" + "=" * 8 + " " + texte + " [" + heure() + "] " + "=" * 8, flush=True)


def session(nom):
    spark = (SparkSession.builder.appName(nom)
             .config("spark.sql.ansi.enabled", "false")           # un cast invalide donne NULL, compte ensuite
             .config("spark.hadoop.dfs.replication", "1")         # un seul DataNode
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.sql.session.timeZone", "Europe/Paris")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def noms(table):
    return [c for c, _ in SCHEMAS[table]]


def lire_brut(spark, table, jeu):
    """Lit un .dat brut (separateur |, sans en-tete, tout en texte) et nomme les colonnes d'apres la specification."""
    df = spark.read.option("sep", "|").option("header", False).option("inferSchema", False) \
        .csv(f"{RAW}/{jeu}/{table}/{table}.dat")
    if len(df.columns) != len(SCHEMAS[table]):
        raise SystemExit(f"{table} : {len(df.columns)} colonnes lues, {len(SCHEMAS[table])} attendues")
    return df.toDF(*noms(table))


def lire_brut_complet(spark, table):
    """Jeu initial + rafraichissement (si present), avec la colonne de provenance `jeu_source`."""
    df = lire_brut(spark, table, "initial").withColumn("jeu_source", F.lit("initial"))
    if table in AVEC_REFRESH:
        df = df.unionByName(lire_brut(spark, table, "refresh").withColumn("jeu_source", F.lit("refresh")))
    return df


def typer(df, table):
    """Conversion explicite dans le type de la specification (chaine vide -> NULL)."""
    exprs = []
    for nom, t in SCHEMAS[table]:
        c = F.when(F.trim(F.col(nom)) == "", F.lit(None)).otherwise(F.col(nom))
        exprs.append((F.trim(c) if t == "string" else c.cast(t)).alias(nom))
    return df.select(*exprs, "jeu_source")
