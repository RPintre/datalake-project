"""Traitement PySpark : Bronze -> Silver -> Gold (extrait des ventes magasin).

Lit les fichiers .dat deposes dans HDFS (zone Bronze), applique les noms de colonnes de la specification TPCx-BB,
type les colonnes, reunit le jeu initial et le rafraichissement, controle la qualite, ecrit en Parquet (Silver),
puis calcule des indicateurs mensuels (Gold).
"""
from pyspark.sql import SparkSession, functions as F
from schemas import SCHEMAS

BASE = "hdfs://namenode:8020/datalake"
spark = (SparkSession.builder.appName("bronze_vers_silver_gold")
         .config("spark.sql.ansi.enabled", "false")  # un cast invalide donne NULL, compte ensuite dans les controles
         .config("spark.hadoop.dfs.replication", "1")  # un seul datanode local : meme reglage que le reste du cluster
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def titre(texte):
    print("\n" + "=" * 8 + " " + texte + " " + "=" * 8)


def lire_bronze(table, jeu):
    """Lit un fichier .dat brut (separateur |, sans en-tete) et nomme les colonnes d'apres la specification."""
    noms = [c for c, _ in SCHEMAS[table]]
    return spark.read.csv(f"{BASE}/bronze/{jeu}/{table}/{table}.dat", sep="|", header=False).toDF(*noms)


def typer(df, table):
    """Convertit chaque colonne dans le type de la specification."""
    exprs = []
    for nom, type_ in SCHEMAS[table]:
        exprs.append(F.col(nom).alias(nom) if type_ == "string" else F.col(nom).cast(type_).alias(nom))
    return df.select(*exprs)


def charger(table, avec_refresh):
    """Charge le jeu initial (et le rafraichissement), les reunit et les type ; controle les conversions."""
    initial = lire_bronze(table, "data")
    n_init = initial.count()
    brut = initial
    n_refr = 0
    if avec_refresh:
        refr = lire_bronze(table, "data_refresh")
        n_refr = refr.count()
        brut = initial.unionByName(refr)
    typed = typer(brut, table)
    invalides = 0
    for nom, type_ in SCHEMAS[table]:
        if type_ != "string":
            invalides += (brut.filter(F.col(nom).isNotNull() & (F.col(nom) != "")).count()
                          - typed.filter(F.col(nom).isNotNull()).count())
    print(f"{table} : {n_init} lignes (jeu initial) + {n_refr} lignes (rafraichissement) = {n_init + n_refr} lignes reunies ; "
          f"conversions de type invalides : {invalides}")
    return typed


# --- SILVER : item, date_dim, store_sales ------------------------------------------------------------------------
titre("SILVER : typage, reunion des deux jeux, controles")
item = charger("item", True)
dates = charger("date_dim", False)
ventes = charger("store_sales", True)

print(f"item : {item.select('i_item_sk').distinct().count()} cles distinctes pour {item.count()} lignes (unicite de la cle)")
print(f"date_dim : {dates.select('d_date_sk').distinct().count()} cles distinctes pour {dates.count()} lignes")
print(f"store_sales : {ventes.select('ss_ticket_number').distinct().count()} tickets distincts")
incoh = ventes.filter(F.abs(F.col("ss_quantity") * F.col("ss_sales_price") - F.col("ss_ext_sales_price")) > 0.011).count()
print(f"coherence quantite x prix de vente = montant : {incoh} ecarts")
orph_item = ventes.join(item, ventes.ss_item_sk == item.i_item_sk, "left_anti").count()
orph_date = ventes.join(dates, ventes.ss_sold_date_sk == dates.d_date_sk, "left_anti").count()
print(f"integrite referentielle ss_item_sk -> item : {orph_item} valeurs sans correspondance")
print(f"integrite referentielle ss_sold_date_sk -> date_dim : {orph_date} valeurs sans correspondance")

item.write.mode("overwrite").parquet(f"{BASE}/silver/item")
dates.write.mode("overwrite").parquet(f"{BASE}/silver/date_dim")
ventes.write.mode("overwrite").parquet(f"{BASE}/silver/store_sales")
print("Tables Silver ecrites en Parquet : item, date_dim, store_sales")

# --- GOLD : indicateurs mensuels des ventes magasin --------------------------------------------------------------
titre("GOLD : indicateurs mensuels des ventes magasin")
s_ventes = spark.read.parquet(f"{BASE}/silver/store_sales")
s_dates = spark.read.parquet(f"{BASE}/silver/date_dim")
kpi = (s_ventes.join(s_dates, s_ventes.ss_sold_date_sk == s_dates.d_date_sk)
       .groupBy("d_year", "d_moy")
       .agg(F.sum("ss_ext_sales_price").alias("chiffre_affaires"),
            F.sum("ss_quantity").alias("quantites"),
            F.countDistinct("ss_ticket_number").alias("transactions"),
            F.sum("ss_ext_discount_amt").alias("remises"),
            F.sum("ss_ext_list_price").alias("prix_catalogue"))
       .withColumn("panier_moyen", F.round(F.col("chiffre_affaires") / F.col("transactions"), 2))
       .withColumn("taux_remise_pct", F.round(100 * F.col("remises") / F.col("prix_catalogue"), 2))
       .drop("remises", "prix_catalogue")
       .orderBy("d_year", "d_moy"))
kpi.write.mode("overwrite").parquet(f"{BASE}/gold/ventes/kpi_mensuel_magasin")
gold = spark.read.parquet(f"{BASE}/gold/ventes/kpi_mensuel_magasin")
print(f"Table Gold ecrite en Parquet : {gold.count()} mois")
gold.show(12, truncate=False)
t = s_ventes.agg(F.sum("ss_ext_sales_price").alias("ca"), F.sum("ss_quantity").alias("qte"),
                 F.countDistinct("ss_ticket_number").alias("tickets"), F.count("*").alias("lignes")).collect()[0]
print(f"Totaux (jeu initial + rafraichissement) : lignes = {t['lignes']}, tickets distincts = {t['tickets']}, "
      f"quantites = {t['qte']}, chiffre d'affaires = {t['ca']}")
spark.stop()
