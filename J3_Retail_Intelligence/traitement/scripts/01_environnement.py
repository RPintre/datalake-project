"""Etape 1 : environnement (versions, mode d'execution, chemins), session Spark, lecture, schema, ecriture de test."""
import os, platform, sys
from commun import *

titre("Environnement")
print("Python        :", platform.python_version())
spark = session("UC06 - environnement")
print("Spark/PySpark :", spark.version)
print("Java          :", spark._jvm.System.getProperty("java.version"))
print("Mode (master) :", spark.sparkContext.master)
print("Coeurs        :", os.cpu_count(), "| parallelisme par defaut :", spark.sparkContext.defaultParallelism)
print("Memoire driver:", spark.sparkContext.getConf().get("spark.driver.memory", "1g (defaut)"))
print("Fuseau        :", spark.conf.get("spark.sql.session.timeZone"))
print("Entree (Raw)  :", RAW)
print("Sorties       :", SILVER, GOLD, QUALITY)

titre("Lecture d'un fichier de test (reason, zone Raw)")
df = lire_brut(spark, "reason", "initial")
df.printSchema()
df.show(5, truncate=False)
print("Lignes :", df.count())

titre("Ecriture de test dans un repertoire dedie (les sources ne sont pas modifiees)")
typer(df.withColumn("jeu_source", F.lit("initial")), "reason").write.mode("overwrite").parquet(TEST + "/reason")
relu = spark.read.parquet(TEST + "/reason")
relu.printSchema()
print("Lignes relues :", relu.count(), "| repertoire :", TEST + "/reason")
spark.stop()
