#!/usr/bin/env python3
"""Construction et pilotage du flux NiFi GetFile -> PutHDFS par l'API REST de NiFi (a lancer dans le conteneur hdfs-client).

Le flux est le meme que celui construit a la main dans l'interface : seule la methode de creation differe (appels REST
au lieu de clics). L'identifiant et le mot de passe NiFi sont fournis par les variables NIFI_USER et NIFI_PASS
(releves dans les journaux de NiFi au demarrage de l instance) et ne sont jamais ecrits dans un fichier.

Commandes : build | start | stop | status | set-input <repertoire> | provenance | export
"""
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "https://nifi:8443/nifi-api"
CTX = ssl._create_unverified_context()  # certificat auto-signe de l'instance locale
OUT = "/sorties/nifi"
IDS = f"{OUT}/ids.json"
os.makedirs(OUT, exist_ok=True)

GETFILE = "org.apache.nifi.processors.standard.GetFile"
PUTHDFS = "org.apache.nifi.processors.hadoop.PutHDFS"
LOGATTR = "org.apache.nifi.processors.standard.LogAttribute"
UPDATEATTR = "org.apache.nifi.processors.attributes.UpdateAttribute"
ROUTEATTR = "org.apache.nifi.processors.standard.RouteOnAttribute"
PROCESSEURS = ("getfile", "puthdfs", "logattr", "maj", "route", "lognr")        # tous les processeurs du flux (les 3 derniers : flux enrichi)
ORDRE_DEMARRAGE = ("puthdfs", "logattr", "lognr", "route", "maj", "getfile")   # destinations d'abord, source en dernier


def appel(methode, chemin, corps=None, formulaire=None, jeton=None, brut=False):
    url = BASE + chemin
    donnees, entetes = None, {}
    if corps is not None:
        donnees, entetes = json.dumps(corps).encode(), {"Content-Type": "application/json"}
    if formulaire is not None:
        donnees, entetes = urllib.parse.urlencode(formulaire).encode(), {"Content-Type": "application/x-www-form-urlencoded"}
    if jeton:
        entetes["Authorization"] = "Bearer " + jeton
    requete = urllib.request.Request(url, data=donnees, headers=entetes, method=methode)
    try:
        with urllib.request.urlopen(requete, context=CTX, timeout=60) as r:
            texte = r.read().decode()
    except urllib.error.HTTPError as e:
        sys.exit(f"Erreur HTTP {e.code} sur {methode} {chemin} : {e.read().decode()[:600]}")
    if brut:
        return texte
    return json.loads(texte) if texte.strip() else {}


def jeton():
    return appel("POST", "/access/token", formulaire={"username": os.environ["NIFI_USER"], "password": os.environ["NIFI_PASS"]}, brut=True)


T = None


def api(methode, chemin, corps=None, brut=False):
    global T
    if T is None:
        T = jeton()
    return appel(methode, chemin, corps=corps, jeton=T, brut=brut)


def charger_ids():
    with open(IDS) as f:
        return json.load(f)


def creer_processeur(groupe, type_, nom, x, y):
    corps = {"revision": {"version": 0}, "component": {"type": type_, "name": nom, "position": {"x": x, "y": y}}}
    return api("POST", f"/process-groups/{groupe}/processors", corps)["id"]


def configurer(pid, proprietes=None, auto_terminees=None, nom=None, planification=None):
    p = api("GET", f"/processors/{pid}")
    config = {}
    if planification is not None:
        config["schedulingPeriod"] = planification
    if proprietes is not None:
        config["properties"] = proprietes
    if auto_terminees is not None:
        config["autoTerminatedRelationships"] = auto_terminees
    composant = {"id": pid, "config": config}
    if nom:
        composant["name"] = nom
    api("PUT", f"/processors/{pid}", {"revision": p["revision"], "component": composant})


def relier(groupe, source, destination, relations):
    corps = {"revision": {"version": 0}, "component": {
        "source": {"id": source, "groupId": groupe, "type": "PROCESSOR"},
        "destination": {"id": destination, "groupId": groupe, "type": "PROCESSOR"},
        "selectedRelationships": relations}}
    return api("POST", f"/process-groups/{groupe}/connections", corps)["id"]


def etat(pid, etat_souhaite):
    p = api("GET", f"/processors/{pid}")
    api("PUT", f"/processors/{pid}/run-status", {"revision": p["revision"], "state": etat_souhaite})


def build():
    racine = api("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]
    groupe = api("POST", f"/process-groups/{racine}/process-groups",
                 {"revision": {"version": 0}, "component": {"name": "UC04_ingestion_raw_nifi", "position": {"x": 100, "y": 100}}})["id"]
    getfile = creer_processeur(groupe, GETFILE, "GetFile_entree", 400, 150)
    puthdfs = creer_processeur(groupe, PUTHDFS, "PutHDFS_raw_nifi", 800, 150)
    logattr = creer_processeur(groupe, LOGATTR, "LogAttribute_echec_depot", 1200, 150)
    # Planification a 10 s : avec la valeur par defaut (0 s), GetFile interroge le dossier en continu.
    configurer(getfile, {"Input Directory": "/data/nifi/input", "Keep Source File": "false", "Recurse Subdirectories": "false"},
               planification="10 sec")
    configurer(puthdfs, {"Hadoop Configuration Resources": "/opt/hadoop-conf/core-site.xml,/opt/hadoop-conf/hdfs-site.xml",
                         "Directory": "/retail-lake/raw/nifi",
                         "Conflict Resolution Strategy": "fail"}, auto_terminees=["success"])
    configurer(logattr, auto_terminees=["success"])
    relier(groupe, getfile, puthdfs, ["success"])
    relier(groupe, puthdfs, logattr, ["failure"])
    ids = {"groupe": groupe, "getfile": getfile, "puthdfs": puthdfs, "logattr": logattr}
    with open(IDS, "w") as f:
        json.dump(ids, f, indent=2)
    print("Flux cree dans le groupe", groupe)
    status()


def status():
    ids = charger_ids()
    for cle in [k for k in PROCESSEURS if k in ids]:
        p = api("GET", f"/processors/{ids[cle]}")
        c = p["component"]
        erreurs = c.get("validationErrors", [])
        print(f"{c['name']:28s} type={c['type'].split('.')[-1]:13s} etat={c['state']:9s} validation={c['validationStatus']}")
        for e in erreurs:
            print("    anomalie de validation :", e)
        stat = api("GET", f"/flow/processors/{ids[cle]}/status")["processorStatus"]["aggregateSnapshot"]
        print(f"    FlowFiles entres={stat['flowFilesIn']} sortis={stat['flowFilesOut']} octets lus={stat['bytesRead']} "
              f"octets ecrits={stat['bytesWritten']} taches={stat['taskCount']}")
        bulletins = api("GET", f"/processors/{ids[cle]}")["bulletins"]
        for b in bulletins[-3:]:
            print(f"    bulletin {b['bulletin']['level']} : {b['bulletin']['message'][:300]}")
    g = api("GET", f"/process-groups/{ids['groupe']}")
    print(f"groupe {g['component']['name']} : en marche={g['runningCount']} arretes={g['stoppedCount']} "
          f"invalides={g['invalidCount']} desactives={g['disabledCount']} ; nombre d'avertissements actifs={g.get('activeRemotePortCount', 0)}")
    cs = api("GET", "/flow/status")["controllerStatus"]
    print(f"instance NiFi : en marche={cs['runningCount']} arretes={cs['stoppedCount']} invalides={cs['invalidCount']} desactives={cs['disabledCount']}")
    conns = api("GET", f"/process-groups/{ids['groupe']}/connections")["connections"]
    for c in conns:
        s = c["status"]["aggregateSnapshot"]
        cc = c["component"]
        print(f"connexion {cc['source']['name']} -> {cc['destination']['name']} {cc['selectedRelationships']} : file d'attente = {s['queued']}")


def demarrer():
    ids = charger_ids()
    for cle in [k for k in ORDRE_DEMARRAGE if k in ids]:
        etat(ids[cle], "RUNNING")
    time.sleep(8)
    status()


def arreter():
    ids = charger_ids()
    for cle in [k for k in PROCESSEURS if k in ids]:
        etat(ids[cle], "STOPPED")
    time.sleep(2)
    status()


def definir_entree(repertoire):
    ids = charger_ids()
    configurer(ids["getfile"], {"Input Directory": repertoire})
    print("Input Directory =", repertoire)
    status()


def provenance():
    ids = charger_ids()
    requete = api("POST", "/provenance", {"provenance": {"request": {"maxResults": 200}}})["provenance"]
    pid = requete["id"]
    for _ in range(30):
        requete = api("GET", f"/provenance/{pid}")["provenance"]
        if requete["finished"]:
            break
        time.sleep(1)
    evenements = requete["results"]["provenanceEvents"]
    api("DELETE", f"/provenance/{pid}")
    evenements.sort(key=lambda e: e["eventId"])
    detail = []
    for e in evenements:
        d = api("GET", f"/provenance-events/{e['id']}")["provenanceEvent"]
        detail.append({"eventId": e["eventId"], "eventTime": e["eventTime"], "eventType": e["eventType"],
                       "componentName": e["componentName"], "flowFileUuid": e["flowFileUuid"], "fileSize": e["fileSize"],
                       "transitUri": d.get("transitUri"), "attributs": {a["name"]: a["value"] for a in d["attributes"]
                                                                       if a["name"] in ("filename", "path", "absolute.path", "uuid", "hdfs.path", "file.size", "jeu", "source", "recu_par")}})
    with open(f"{OUT}/provenance_evenements.json", "w") as f:
        json.dump(detail, f, indent=2, ensure_ascii=False)
    print(f"{'id':>4} {'type':8s} {'composant':26s} {'fichier':16s} {'octets':>8s}  heure")
    for d in detail:
        print(f"{d['eventId']:>4} {d['eventType']:8s} {d['componentName']:26s} {d['attributs'].get('filename',''):16s} {d['fileSize']:>8}  {d['eventTime']}")
    print("uuid FlowFile :", sorted({d["flowFileUuid"] for d in detail}))


def enrichir():
    """Etape 12 du TP : enrichit le flux avec UpdateAttribute (attributs de suivi), RouteOnAttribute (routage selon le nom du
    fichier) et un second LogAttribute (fichiers non reconnus). Le flux doit etre arrete et ses files d'attente vides."""
    ids = charger_ids()
    groupe = ids["groupe"]
    for c in api("GET", f"/process-groups/{groupe}/connections")["connections"]:
        cc = c["component"]
        if cc["source"]["id"] == ids["getfile"] and cc["destination"]["id"] == ids["puthdfs"]:
            api("DELETE", f"/connections/{c['id']}?version={c['revision']['version']}")
            print("Connexion directe GetFile -> PutHDFS remplacee par le flux enrichi")
    maj = creer_processeur(groupe, UPDATEATTR, "UpdateAttribute_suivi", 400, 420)
    route = creer_processeur(groupe, ROUTEATTR, "RouteOnAttribute_domaine", 800, 420)
    lognr = creer_processeur(groupe, LOGATTR, "LogAttribute_non_reconnu", 1200, 420)
    configurer(maj, {"jeu": "initial", "source": "tpcxbb", "recu_par": "GetFile_entree"})
    configurer(route, {"Routing Strategy": "Route to Property name",
                       "tables_reference": "${filename:startsWith('reason'):or(${filename:startsWith('ship_mode')}):or(${filename:startsWith('income_band')})}"})
    configurer(lognr, auto_terminees=["success"])
    relier(groupe, ids["getfile"], maj, ["success"])
    relier(groupe, maj, route, ["success"])
    relier(groupe, route, ids["puthdfs"], ["tables_reference"])
    relier(groupe, route, lognr, ["unmatched"])
    ids.update({"maj": maj, "route": route, "lognr": lognr})
    with open(IDS, "w") as f:
        json.dump(ids, f, indent=2)
    print("Flux enrichi cree : GetFile -> UpdateAttribute -> RouteOnAttribute -> (tables_reference) PutHDFS | (unmatched) LogAttribute")
    status()


def decouverte():
    """Releve de l'instance : version, types de processeurs disponibles, composants utilises par le flux."""
    about = api("GET", "/flow/about")["about"]
    print(f"Instance : {about['title']} version {about['version']}")
    types = api("GET", "/flow/processor-types")["processorTypes"]
    print(f"Types de processeurs disponibles : {len(types)}")
    for nom in ("GetFile", "PutHDFS", "LogAttribute"):
        trouves = [t for t in types if t["type"].endswith("." + nom)]
        for t in trouves:
            print(f"- {nom} : disponible ({t['bundle']['artifact']} {t['bundle']['version']})")
        if not trouves:
            print(f"- {nom} : ABSENT")
    cs = api("GET", "/flow/status")["controllerStatus"]
    print(f"Etat general : en marche={cs['runningCount']} arretes={cs['stoppedCount']} invalides={cs['invalidCount']} desactives={cs['disabledCount']}")
    racine = api("GET", "/flow/process-groups/root")["processGroupFlow"]
    print(f"Groupe racine : {racine['id']} ; processeurs={len(racine['flow']['processors'])} groupes={len(racine['flow']['processGroups'])}")


def exporter():
    ids = charger_ids()
    definition = api("GET", f"/process-groups/{ids['groupe']}/download", brut=True)
    with open(f"{OUT}/flow_definition.json", "w") as f:
        f.write(definition)
    flux = json.loads(definition)
    contenu = flux.get("flowContents", flux)
    print("Definition exportee :", f"{OUT}/flow_definition.json")
    for p in contenu["processors"]:
        print(f"- {p['name']} ({p['type'].split('.')[-1]}) relations auto-terminees={p.get('autoTerminatedRelationships')}")
        for k, v in p["properties"].items():
            if v not in (None, ""):
                print(f"    {k} = {v}")
    for c in contenu["connections"]:
        print(f"- connexion {c['selectedRelationships']} : {c['source']['name']} -> {c['destination']['name']}")


if __name__ == "__main__":
    commande = sys.argv[1] if len(sys.argv) > 1 else "status"
    if commande == "build":
        build()
    elif commande == "start":
        demarrer()
    elif commande == "stop":
        arreter()
    elif commande == "status":
        status()
    elif commande == "set-input":
        definir_entree(sys.argv[2])
    elif commande == "positions":
        ids = charger_ids()
        for cle, x in (("getfile", 400), ("puthdfs", 800), ("logattr", 1200)):
            p = api("GET", f"/processors/{ids[cle]}")
            api("PUT", f"/processors/{ids[cle]}", {"revision": p["revision"], "component": {"id": ids[cle], "position": {"x": x, "y": 150}}})
        print("Positions mises a jour")
    elif commande == "set-schedule":
        configurer(charger_ids()["getfile"], planification=sys.argv[2])
        print("Planification de GetFile =", sys.argv[2])
    elif commande == "provenance":
        provenance()
    elif commande == "export":
        exporter()
    elif commande == "decouverte":
        decouverte()
    elif commande == "enrichir":
        enrichir()
    else:
        sys.exit("Commande inconnue : " + commande)
