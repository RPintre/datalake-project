"""Etape 4 : application des regles de nettoyage (R1 a R6) et mesure de leur effet. Rien n'est ecrit dans Silver ici."""
from commun import *
from nettoyage import *

spark = session("UC06 - nettoyage")
titre("R1 Types : echecs de conversion (valeur non vide devenue NULL)")
for t in ["store_sales", "store_returns", "item", "store", "date_dim", "reason"]:
    e = {c: v for c, v in echecs_conversion(spark, t).items() if v}
    print(f"{t:14s} {e if e else 'aucun echec'}")

titre("Dimensions nettoyees (R1, R3, cle obligatoire, R6)")
dims = {}
for t in ["item", "store", "reason", "date_dim"]:
    brut = charger_type(spark, t)
    dims[t] = dimension(spark, t).cache()
    print(f"{t:10s} lues : {brut.count():>8d} | conservees : {dims[t].count():>8d}")

titre("Tables de faits : R3 doublons exacts, R2 cle obligatoire, R5 references")
for t in ["store_sales", "store_returns"]:
    brut = charger_type(spark, t)
    n0 = brut.count()
    n1 = dedoublonner_exact(brut).count()
    silver, q = faits(spark, t, dims)
    ns, nq = silver.count(), q.count()
    print(f"{t} : {n0} lignes lues, {n0 - n1} doublon(s) exact(s) retire(s) (R3), {ns} conservees, {nq} en quarantaine "
          f"(controle : {n1} = {ns} + {nq} -> {'OK' if n1 == ns + nq else 'ECART'})")
    if nq:
        q.groupBy("motif_quarantaine").count().show(truncate=False)

titre("R4 Montants et quantites negatifs ou nuls : conserves, comptes")
ss, _ = faits(spark, "store_sales", dims)
print("store_sales quantite <= 0 :", ss.filter("ss_quantity <= 0").count(),
      "| prix de vente < 0 :", ss.filter("ss_sales_price < 0").count(),
      "| profit net < 0 (vente a perte, conservee) :", ss.filter("ss_net_profit < 0").count())
spark.stop()
