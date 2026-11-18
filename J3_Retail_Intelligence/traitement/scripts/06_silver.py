"""Etape 6 : ecriture de la zone Silver (Parquet) : dimensions, faits nettoyes et quarantaine, avec controle apres ecriture."""
from commun import *
from nettoyage import *

spark = session("UC06 - silver")
dims = {t: dimension(spark, t) for t in ["item", "store", "reason", "date_dim"]}
for t in dims:
    dims[t] = dims[t].cache()
sorties = {f"{SILVER}/{t}": (df, df.count()) for t, df in dims.items()}
for t in ["store_sales", "store_returns"]:
    ok, q = faits(spark, t, dims)
    sorties[f"{SILVER}/{t}"] = (ok, ok.count())
    sorties[f"{SILVER}/_quarantaine/{t}"] = (q, q.count())

titre("Ecriture Silver (Parquet, mode overwrite sur la zone Silver dediee)")
for chemin, (df, n) in sorties.items():
    df.write.mode("overwrite").parquet(chemin)
    print(f"ecrit : {chemin} ({n} lignes attendues)")

titre("Controle apres ecriture : presence, format, lisibilite, lignes, schema")
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(RACINE), spark._jsc.hadoopConfiguration())
for chemin, (df, n) in sorties.items():
    p = spark._jvm.org.apache.hadoop.fs.Path(chemin)
    relu = spark.read.parquet(chemin)
    nb_fichiers = len([f for f in fs.listStatus(p) if f.getPath().getName().endswith(".parquet")])
    octets = fs.getContentSummary(p).getLength()
    ok = relu.count() == n and relu.columns == df.columns
    print(f"{chemin.replace(RACINE, ''):32s} {nb_fichiers} fichier(s) parquet, {octets:>11d} octets, {relu.count():>8d} lignes, "
          f"{len(relu.columns)} colonnes -> {'OK' if ok else 'ECART'}")
titre("Schema Silver : store_sales")
spark.read.parquet(f"{SILVER}/store_sales").printSchema()
spark.stop()
