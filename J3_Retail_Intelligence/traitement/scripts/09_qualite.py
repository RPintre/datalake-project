"""Etape 9 : controles qualite sur Silver et Gold ; ecrit le bilan dans /retail-lake/quality/rapport_qualite_uc06."""
from commun import *
from nettoyage import *

spark = session("UC06 - qualite")
lire = lambda c: spark.read.parquet(c)
ss, sr = lire(f"{SILVER}/store_sales"), lire(f"{SILVER}/store_returns")
item, store, dates, reason = (lire(f"{SILVER}/{t}") for t in ["item", "store", "date_dim", "reason"])
qss, qsr = lire(f"{SILVER}/_quarantaine/store_sales"), lire(f"{SILVER}/_quarantaine/store_returns")
g1, g2, g3 = (lire(f"{GOLD}/{n}") for n in ["ventes_par_mois_magasin", "ventes_par_mois_categorie", "retours_par_motif"])
resultats = []


def controle(famille, regle, valeur, attendu_ok, detail=""):
    statut = "OK" if attendu_ok else "ANOMALIE"
    resultats.append((famille, regle, str(valeur), statut, detail))
    print(f"[{statut:8s}] {famille:12s} {regle:62s} {valeur} {detail}", flush=True)


titre("Completude : champs obligatoires non nuls")
for nom, df, cols in [("store_sales", ss, OBLIGATOIRES["store_sales"]), ("store_returns", sr, OBLIGATOIRES["store_returns"])]:
    for c in cols:
        n = df.filter(F.col(c).isNull()).count()
        controle("Completude", f"{nom}.{c} non nul", n, n == 0, "nulls")
titre("Unicite : cles candidates")
for nom, df, cle in [("item", item, ["i_item_sk"]), ("store", store, ["s_store_sk"]), ("date_dim", dates, ["d_date_sk"]),
                     ("reason", reason, ["r_reason_sk"])]:
    rep = df.count() - df.dropDuplicates(cle).count()
    controle("Unicite", f"{nom} : cle {cle} unique", rep, rep == 0, "repetitions")
for nom, df, cle in [("store_sales", ss, CLES["store_sales"]), ("store_returns", sr, CLES["store_returns"])]:
    rep = df.count() - df.dropDuplicates(cle).count()
    controle("Unicite", f"{nom} : cle composite {cle}", rep, True, "repetitions signalees, lignes de valeurs differentes conservees (R3)")
titre("Validite des types : schema relu = schema attendu")
for nom, df in [("store_sales", ss), ("store_returns", sr), ("item", item), ("store", store), ("reason", reason)]:
    attendu = {c: t.replace("decimal", "decimal") for c, t in SCHEMAS[nom]}
    lu = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    ecarts = [c for c, t in attendu.items() if t != "string" and lu.get(c) != t.replace("bigint", "bigint")]
    controle("Validite", f"{nom} : types conformes a la specification", len(ecarts), not ecarts, str(ecarts[:3]) if ecarts else "")
titre("Coherence des valeurs")
n = ss.filter("ss_quantity <= 0").count()
controle("Coherence", "store_sales : quantite > 0", n, n == 0, "lignes a quantite <= 0 (conservees, R4)")
n = ss.filter("ss_sales_price < 0").count()
controle("Coherence", "store_sales : prix de vente >= 0", n, n == 0)
n = ss.filter("ss_sales_price = 0").count()
controle("Coherence", "store_sales : prix de vente non nul (information, limite des donnees)", n, True, "lignes a prix 0, conservees (R4) et a interpreter avec prudence")
n = ss.filter(F.abs(F.col("ss_ext_sales_price") - F.col("ss_quantity") * F.col("ss_sales_price")) > 0.011).count()
controle("Coherence", "store_sales : montant = quantite x prix unitaire (tolerance 0,01)", n, n == 0, "lignes en ecart")
n = sr.filter("sr_return_quantity <= 0").count()
controle("Coherence", "store_returns : quantite retournee > 0", n, n == 0)
n = dates.filter("d_moy < 1 or d_moy > 12").count()
controle("Coherence", "date_dim : mois entre 1 et 12", n, n == 0)
titre("Volumes : lignes lues = lignes Silver + quarantaine + doublons exacts")
dims = {t: dimension(spark, t) for t in ["item", "store", "reason", "date_dim"]}
for nom, df, q in [("store_sales", ss, qss), ("store_returns", sr, qsr)]:
    brut = charger_type(spark, nom)
    n0, n1 = brut.count(), dedoublonner_exact(brut).count()
    controle("Volume", f"{nom} : {n0} lues = {df.count()} Silver + {q.count()} quarantaine + {n0 - n1} doublons",
             df.count() + q.count() + (n0 - n1), df.count() + q.count() + (n0 - n1) == n0)
titre("Integrite referentielle (Silver)")
for nom, df, cf, dim, cd in [("store_sales", ss, "ss_item_sk", item, "i_item_sk"), ("store_sales", ss, "ss_store_sk", store, "s_store_sk"),
                             ("store_sales", ss, "ss_sold_date_sk", dates, "d_date_sk"), ("store_returns", sr, "sr_item_sk", item, "i_item_sk")]:
    orph = df.join(dim.select(cd), df[cf] == dim[cd], "left_anti").count()
    controle("Integrite", f"{nom}.{cf} -> {cd}", orph, orph == 0, "orphelins")
n = sr.filter("sr_reason_sk is not null").join(reason, sr.sr_reason_sk == reason.r_reason_sk, "left_anti").count()
controle("Integrite", "store_returns.sr_reason_sk -> r_reason_sk (motifs renseignes)", n, n == 0, "orphelins")
titre("Coherence Gold / Silver")
ca_s = ss.join(dates.select("d_date_sk"), ss.ss_sold_date_sk == dates.d_date_sk).join(store.select("s_store_sk"), ss.ss_store_sk == store.s_store_sk) \
         .agg(F.sum("ss_ext_sales_price")).first()[0]
ca_g1, ca_g2 = g1.agg(F.sum("chiffre_affaires")).first()[0], g2.agg(F.sum("chiffre_affaires")).first()[0]
controle("Gold", "somme CA Gold magasin = somme CA Silver", f"{ca_g1} / {ca_s}", ca_g1 == ca_s)
controle("Gold", "somme CA Gold categorie = somme CA Silver", f"{ca_g2} / {ca_s}", ca_g2 == ca_s)
q_s, q_g = sr.agg(F.sum("sr_return_quantity")).first()[0], g3.agg(F.sum("quantite_retournee")).first()[0]
controle("Gold", "quantites retournees Gold = Silver", f"{q_g} / {q_s}", q_g == q_s)
titre("Bilan")
ok = sum(1 for r in resultats if r[3] == "OK")
print(f"{ok} controle(s) OK sur {len(resultats)} ; anomalies : {len(resultats) - ok}")
for r in resultats:
    if r[3] != "OK":
        print("  ANOMALIE :", r[0], "-", r[1], "->", r[2], r[4])
rap = spark.createDataFrame(resultats, ["famille", "regle", "resultat", "statut", "detail"])
rap.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{QUALITY}/rapport_qualite_uc06")
print("Rapport ecrit :", f"{QUALITY}/rapport_qualite_uc06")
spark.stop()
