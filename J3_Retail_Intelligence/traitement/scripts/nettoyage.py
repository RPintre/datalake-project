"""Regles de nettoyage UC-06 (voir regles_nettoyage) : fonctions appliquees par les etapes 4 a 9.

R1 Types      : conversion explicite selon la specification ; chaine vide -> NULL ; un echec de conversion donne NULL et est compte.
R2 Obligatoire: une ligne de faits sans cle obligatoire (ticket, article, date, magasin) est mise en quarantaine, jamais supprimee.
R3 Doublons   : seule une ligne strictement identique (toutes colonnes, jeu initial et rafraichissement compris) est dedoublonnee.
                Deux lignes de meme cle mais de valeurs differentes sont conservees et signalees.
R4 Montants   : quantites et montants negatifs ou nuls conserves (retour, remise, correction) ; ils sont comptes, pas modifies.
R5 Reference  : une ligne dont l'article, la date ou le magasin n'existe pas dans la dimension est mise en quarantaine.
R6 Dates      : d_date (texte AAAA-MM-JJ) converti en date.
"""
from pyspark.sql import functions as F
from commun import SCHEMAS, CLES, lire_brut_complet, typer, noms

OBLIGATOIRES = {
    "store_sales": ["ss_ticket_number", "ss_item_sk", "ss_sold_date_sk", "ss_store_sk"],
    "store_returns": ["sr_ticket_number", "sr_item_sk", "sr_returned_date_sk", "sr_store_sk"],
}


def charger_type(spark, table):
    """R1 : lecture brute (initial + rafraichissement) puis conversion explicite des types."""
    return typer(lire_brut_complet(spark, table), table)


def echecs_conversion(spark, table):
    """Nombre de valeurs non vides qui deviennent NULL apres conversion, par colonne (controle de R1)."""
    brut = lire_brut_complet(spark, table)
    typed = typer(brut, table)
    res = {}
    # une seule passe par table : comparaison sur les colonnes non texte
    cols = [nom for nom, t in SCHEMAS[table] if t != "string"]
    b = brut.select(*[(F.col(c).isNotNull() & (F.trim(F.col(c)) != "")).cast("int").alias(c) for c in cols])
    t_ = typed.select(*[F.col(c).isNotNull().cast("int").alias(c) for c in cols])
    sb = b.agg(*[F.sum(c).alias(c) for c in cols]).first()
    st = t_.agg(*[F.sum(c).alias(c) for c in cols]).first()
    for c in cols:
        res[c] = int((sb[c] or 0) - (st[c] or 0))
    return res


def dedoublonner_exact(df):
    """R3 : suppression des seules lignes strictement identiques sur toutes les colonnes de donnees."""
    colonnes = [c for c in df.columns if c != "jeu_source"]
    return df.dropDuplicates(colonnes)


def separer_obligatoires(df, table):
    """R2 : (lignes valides, lignes en quarantaine pour cle obligatoire absente)."""
    cond = None
    for c in OBLIGATOIRES[table]:
        t = F.col(c).isNotNull()
        cond = t if cond is None else (cond & t)
    return df.filter(cond), df.filter(~cond).withColumn("motif_quarantaine", F.lit("cle obligatoire absente"))


def separer_references(df, dim, cle_fait, cle_dim, motif):
    """R5 : (lignes rapprochees, lignes orphelines) par anti-jointure sur la cle de dimension verifiee."""
    cles = dim.select(F.col(cle_dim).alias("_k")).distinct()
    j = df.join(cles, df[cle_fait] == cles["_k"], "left")
    return j.filter(F.col("_k").isNotNull()).drop("_k"), \
        j.filter(F.col("_k").isNull()).drop("_k").withColumn("motif_quarantaine", F.lit(motif))


def dimension(spark, table):
    """Dimension nettoyee : typage, dedoublonnage exact, cle obligatoire (item, store, reason, date_dim)."""
    df = dedoublonner_exact(charger_type(spark, table))
    cle = CLES[table][0]
    df = df.filter(F.col(cle).isNotNull())
    if table == "date_dim":
        df = df.withColumn("d_date", F.to_date("d_date", "yyyy-MM-dd"))          # R6
    return df


def faits(spark, table, dims):
    """Table de faits nettoyee : (silver, quarantaine) apres R1, R3, R2 puis R5."""
    df = dedoublonner_exact(charger_type(spark, table))
    ok, q1 = separer_obligatoires(df, table)
    p = "ss" if table == "store_sales" else "sr"
    date_col = f"{p}_sold_date_sk" if p == "ss" else f"{p}_returned_date_sk"
    ok, q2 = separer_references(ok, dims["item"], f"{p}_item_sk", "i_item_sk", "article inconnu")
    ok, q3 = separer_references(ok, dims["date_dim"], date_col, "d_date_sk", "date inconnue")
    ok, q4 = separer_references(ok, dims["store"], f"{p}_store_sk", "s_store_sk", "magasin inconnu")
    q = q1.unionByName(q2).unionByName(q3).unionByName(q4)
    ok = ok.withColumn("date_traitement", F.current_date())
    return ok, q
