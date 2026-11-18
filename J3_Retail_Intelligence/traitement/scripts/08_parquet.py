"""Etape 8 : Parquet : relecture, schema, volumes et comparaison avec le CSV brut (taille, temps d'une agregation)."""
import time
from commun import *
from nettoyage import charger_type

spark = session("UC06 - parquet")
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(RACINE), spark._jsc.hadoopConfiguration())
taille = lambda c: fs.getContentSummary(spark._jvm.org.apache.hadoop.fs.Path(c)).getLength()

titre("Relecture Parquet : Silver store_sales")
p = spark.read.parquet(f"{SILVER}/store_sales")
p.printSchema()
p.show(5, truncate=False)
print("Lignes relues :", p.count())

titre("Comparaison CSV brut / Parquet (store_sales, memes lignes : jeu initial + rafraichissement)")
csv_octets = taille(f"{RAW}/initial/store_sales") + taille(f"{RAW}/refresh/store_sales")
pq_octets = taille(f"{SILVER}/store_sales")
print(f"Taille CSV (.dat)          : {csv_octets:>12d} octets")
print(f"Taille Parquet (Silver)    : {pq_octets:>12d} octets  (rapport {pq_octets / csv_octets:.2f})")


def chrono(f):
    t = time.time(); r = f(); return r, time.time() - t

csv = charger_type(spark, "store_sales")
r1, t1 = chrono(lambda: csv.agg(F.sum("ss_ext_sales_price")).first()[0])
r2, t2 = chrono(lambda: p.agg(F.sum("ss_ext_sales_price")).first()[0])
print(f"Somme de ss_ext_sales_price : CSV {r1} en {t1:.2f} s | Parquet {r2} en {t2:.2f} s")
print("Note : le CSV conserve les lignes de la quarantaine et les doublons ; la somme Parquet porte sur Silver. "
      "Les durees dependent de la machine, du cache et de la taille : elles sont donnees a titre indicatif.")
print("JSON : non disponible dans le jeu (les sources sont en CSV) -> non compare.")
titre("Parquet seul n'est pas une table transactionnelle")
print("Un repertoire Parquet n'offre ni transaction (ACID), ni historique de versions, ni suppression ou mise a jour de lignes : "
      "ces garanties demandent un format de table Lakehouse (Delta Lake, Iceberg, Hudi), non utilise ici.")
spark.stop()
