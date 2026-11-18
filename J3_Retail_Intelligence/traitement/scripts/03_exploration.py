"""Etape 3 : exploration PySpark (schema, echantillon, volumes, valeurs manquantes, doublons, valeurs atypiques)."""
from commun import *
from nettoyage import charger_type, echecs_conversion

SOURCES = ["store_sales", "store_returns", "item", "store", "date_dim", "reason"]
spark = session("UC06 - exploration")

for t in SOURCES:
    titre(f"Exploration : {t}")
    df = charger_type(spark, t).cache()
    n = df.count()
    print("Lignes :", n, "| colonnes :", len(df.columns) - 1)
    df.printSchema()
    df.show(5, truncate=40)

    # valeurs manquantes par colonne (une seule passe)
    cols = noms(t)
    manq = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first()
    print("Valeurs nulles par colonne (colonnes concernees) :")
    for c in cols:
        if manq[c]:
            print(f"   {c:25s} {manq[c]:>10d} ({100 * manq[c] / n:.2f} %)")
    if not any(manq[c] for c in cols):
        print("   aucune")

    # echecs de conversion de types
    ech = {c: v for c, v in echecs_conversion(spark, t).items() if v}
    print("Echecs de conversion de type :", ech if ech else "aucun")

    # doublons
    exacts = n - df.dropDuplicates(cols).count()
    cle = CLES[t]
    sur_cle = n - df.dropDuplicates(cle).count()
    print(f"Doublons : {exacts} ligne(s) strictement identique(s) ; {sur_cle} repetition(s) de la cle {cle}")

    # valeurs atypiques
    if t == "store_sales":
        df.agg(F.min("ss_quantity").alias("qte_min"), F.max("ss_quantity").alias("qte_max"),
               F.min("ss_sales_price").alias("prix_min"), F.max("ss_sales_price").alias("prix_max"),
               F.min("ss_net_profit").alias("profit_min"), F.max("ss_net_profit").alias("profit_max")).show()
        print("Quantite <= 0 :", df.filter("ss_quantity <= 0").count(), "| prix de vente < 0 :", df.filter("ss_sales_price < 0").count(),
              "| profit net < 0 :", df.filter("ss_net_profit < 0").count())
        print("Prix de vente = 0 (remise totale ou donnee synthetique) :", df.filter("ss_sales_price = 0").count(),
              "| montant net paye = 0 :", df.filter("ss_net_paid = 0").count())
        print("Ecart ss_ext_sales_price != quantite x prix :",
              df.filter(F.abs(F.col("ss_ext_sales_price") - F.col("ss_quantity") * F.col("ss_sales_price")) > 0.011).count())
    elif t == "store_returns":
        df.agg(F.min("sr_return_quantity").alias("qte_min"), F.max("sr_return_quantity").alias("qte_max"),
               F.min("sr_return_amt").alias("montant_min"), F.max("sr_return_amt").alias("montant_max")).show()
        print("Quantite retournee <= 0 :", df.filter("sr_return_quantity <= 0").count())
    elif t == "item":
        df.agg(F.min("i_current_price").alias("prix_min"), F.max("i_current_price").alias("prix_max")).show()
        print("Prix courant <= 0 :", df.filter("i_current_price <= 0").count())
    elif t == "date_dim":
        df.agg(F.min("d_year").alias("annee_min"), F.max("d_year").alias("annee_max"),
               F.min("d_date").alias("date_min"), F.max("d_date").alias("date_max")).show()
    df.unpersist()
spark.stop()
