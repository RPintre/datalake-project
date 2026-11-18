"""Etape 7 : zone Gold (Parquet), jeux analytiques construits depuis Silver uniquement."""
from commun import *

spark = session("UC06 - gold")
ss = spark.read.parquet(f"{SILVER}/store_sales")
sr = spark.read.parquet(f"{SILVER}/store_returns")
item = spark.read.parquet(f"{SILVER}/item")
store = spark.read.parquet(f"{SILVER}/store")
dates = spark.read.parquet(f"{SILVER}/date_dim")
reason = spark.read.parquet(f"{SILVER}/reason")

def annee_mois():
    return F.format_string("%04d-%02d", F.col("d_year"), F.col("d_moy")).alias("annee_mois")

# Gold 1 : ventes par mois et par magasin
v = ss.join(dates.select("d_date_sk", "d_year", "d_moy"), ss.ss_sold_date_sk == dates.d_date_sk) \
      .join(store.select("s_store_sk", "s_store_name", "s_state"), ss.ss_store_sk == store.s_store_sk)
gold1 = (v.groupBy(annee_mois(), "s_store_sk", "s_store_name", "s_state")
          .agg(F.count("*").alias("nb_lignes_vente"), F.sum("ss_quantity").alias("quantite_vendue"),
               F.sum("ss_ext_sales_price").alias("chiffre_affaires"), F.sum("ss_net_profit").alias("profit_net"))
          .withColumn("date_traitement", F.current_date()))

# Gold 2 : ventes par mois et par categorie de produit
v2 = ss.join(dates.select("d_date_sk", "d_year", "d_moy"), ss.ss_sold_date_sk == dates.d_date_sk) \
       .join(item.select("i_item_sk", "i_category"), ss.ss_item_sk == item.i_item_sk, "left")
gold2 = (v2.groupBy(annee_mois(), F.coalesce("i_category", F.lit("Non renseignee")).alias("categorie"))
           .agg(F.count("*").alias("nb_lignes_vente"), F.sum("ss_quantity").alias("quantite_vendue"),
                F.sum("ss_ext_sales_price").alias("chiffre_affaires"))
           .withColumn("date_traitement", F.current_date()))

# Gold 3 : retours par motif
gold3 = (sr.join(reason, sr.sr_reason_sk == reason.r_reason_sk, "left")
           .groupBy(F.coalesce("r_reason_desc", F.lit("Motif non renseigne")).alias("motif"))
           .agg(F.count("*").alias("nb_lignes_retour"), F.sum("sr_return_quantity").alias("quantite_retournee"),
                F.sum("sr_return_amt").alias("montant_retourne"))
           .withColumn("date_traitement", F.current_date()))

titre("Ecriture Gold (Parquet)")
for nom, g in [("ventes_par_mois_magasin", gold1), ("ventes_par_mois_categorie", gold2), ("retours_par_motif", gold3)]:
    g.write.mode("overwrite").parquet(f"{GOLD}/{nom}")
    r = spark.read.parquet(f"{GOLD}/{nom}")
    print(f"{GOLD.replace(RACINE, '')}/{nom} : {r.count()} lignes, colonnes : {', '.join(r.columns)}")
titre("Apercu des jeux Gold")
spark.read.parquet(f"{GOLD}/ventes_par_mois_magasin").orderBy("annee_mois", "s_store_sk").show(8, truncate=False)
spark.read.parquet(f"{GOLD}/ventes_par_mois_categorie").orderBy("annee_mois", "categorie").show(8, truncate=False)
spark.read.parquet(f"{GOLD}/retours_par_motif").orderBy(F.desc("nb_lignes_retour")).show(10, truncate=False)
spark.stop()
