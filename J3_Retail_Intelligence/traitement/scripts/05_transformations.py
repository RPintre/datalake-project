"""Etape 5 : transformations (colonnes calculees, jointures verifiees, agregations). Lecture seule, aucune ecriture Silver/Gold."""
from commun import *
from nettoyage import *

spark = session("UC06 - transformations")
dims = {t: dimension(spark, t).cache() for t in ["item", "store", "reason", "date_dim"]}
ss, _ = faits(spark, "store_sales", dims)
sr, _ = faits(spark, "store_returns", dims)
ss, sr = ss.cache(), sr.cache()

titre("Verification des cles de jointure (unicite cote dimension)")
for t, cle in [("item", "i_item_sk"), ("store", "s_store_sk"), ("date_dim", "d_date_sk"), ("reason", "r_reason_sk")]:
    n, d = dims[t].count(), dims[t].select(cle).distinct().count()
    print(f"{t:10s} {cle:12s} {n} lignes, {d} valeurs distinctes -> relation plusieurs-a-un {'verifiee' if n == d else 'NON verifiee'}")

titre("Jointures : ventes x article, date, magasin (jointures gauches)")
n_avant = ss.count()
j = ss.join(dims["item"].select("i_item_sk", "i_category", "i_class", "i_brand", "i_product_name"),
            ss.ss_item_sk == dims["item"].i_item_sk, "left")
print("ventes + article  : avant", n_avant, "| apres", j.count(), "| non rapprochees :", j.filter("i_item_sk is null").count())
j = j.join(dims["date_dim"].select("d_date_sk", "d_date", "d_year", "d_moy"), j.ss_sold_date_sk == dims["date_dim"].d_date_sk, "left")
print("   + date         : apres", j.count(), "| non rapprochees :", j.filter("d_date_sk is null").count())
j = j.join(dims["store"].select("s_store_sk", "s_store_name", "s_state"), j.ss_store_sk == dims["store"].s_store_sk, "left")
print("   + magasin      : apres", j.count(), "| non rapprochees :", j.filter("s_store_sk is null").count())
print("Cardinalite : chaque vente a au plus une ligne par dimension -> le nombre de lignes doit rester", n_avant)

titre("Jointure : retours x motif")
n_r = sr.count()
jr = sr.join(dims["reason"], sr.sr_reason_sk == dims["reason"].r_reason_sk, "left")
print("retours : avant", n_r, "| apres", jr.count(), "| sans motif rapproche :", jr.filter("r_reason_sk is null").count(),
      "| motif absent dans le fait :", sr.filter("sr_reason_sk is null").count())

titre("Colonnes calculees et agregations (apercu)")
j = j.withColumn("annee_mois", F.format_string("%04d-%02d", F.col("d_year"), F.col("d_moy"))) \
     .withColumn("marge_brute", F.col("ss_ext_sales_price") - F.col("ss_ext_wholesale_cost"))
(j.groupBy("annee_mois").agg(F.count("*").alias("lignes"), F.sum("ss_quantity").alias("quantite"),
                             F.sum("ss_ext_sales_price").alias("ca"))
  .orderBy("annee_mois").show(12, truncate=False))
print("Granularite : une ligne = un mois ; ca = somme de ss_ext_sales_price (montant de vente hors taxe).")
(jr.groupBy("r_reason_desc").agg(F.count("*").alias("retours"), F.sum("sr_return_quantity").alias("quantite"),
                                 F.sum("sr_return_amt").alias("montant"))
   .orderBy(F.desc("retours")).show(10, truncate=False))
spark.stop()
