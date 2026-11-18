"""Etape 2 : inventaire technique des sources retenues (fichier, emplacement, format, taille, lignes, colonnes, cle, nulls)."""
from commun import *
from nettoyage import charger_type

SOURCES = ["store_sales", "store_returns", "item", "store", "date_dim", "reason"]
spark = session("UC06 - inventaire")
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(RACINE), spark._jsc.hadoopConfiguration())


def taille(chemin):
    return fs.getContentSummary(spark._jvm.org.apache.hadoop.fs.Path(chemin)).getLength()


titre("Inventaire des sources (zone Raw : jeu initial + rafraichissement)")
lignes = []
for t in SOURCES:
    jeux = ["initial"] + (["refresh"] if t in AVEC_REFRESH else [])
    for j in jeux:
        brut = lire_brut(spark, t, j)
        octets = f"{taille(f'{RAW}/{j}/{t}/{t}.dat'):,d}".replace(",", " ")
        lg = f"{brut.count():,d}".replace(",", " ")
        print(f"{t:14s} {j:8s} {RAW}/{j}/{t}/{t}.dat | CSV sans en-tete, separateur | | "
              f"{octets:>12s} octets | {lg:>9s} lignes | {len(brut.columns)} colonnes")
    typed = charger_type(spark, t)
    cle = CLES[t]
    n = typed.count()
    distincts = typed.select(*cle).distinct().count()
    nulls_cle = typed.filter(" or ".join(f"{c} is null" for c in cle)).count()
    print(f"   -> {t} : {n} lignes reunies ; cle candidate {cle} : {distincts} valeurs distinctes "
          f"({'unique' if distincts == n else 'NON unique : ' + str(n - distincts) + ' repetitions'}), {nulls_cle} cle(s) nulle(s)")
    print("   colonnes principales et types :", ", ".join(f"{c}:{ty}" for c, ty in SCHEMAS[t][:6]), "...")
spark.stop()
