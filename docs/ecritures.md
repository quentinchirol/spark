# Chemin d'écriture : 10 000 écritures par seconde

```
application ─HTTP─► passerelle ─acks=all─► Kafka « app » (24 partitions, clé = id) ─lots─► writers ─► PostgreSQL principal
  (202 en ms)        (2 → 12 pods)          (3 nœuds, 3 zones, 2 copies mini)          (KEDA 2 → 24)     (vue lisible ~1 s)
```

| Étape | Composant | Garantie |
|-------|-----------|----------|
| Accepter | passerelle (`python/passerelle`) ou API de l'application | 202 seulement quand Kafka a écrit le message sur 2 nœuds au moins |
| Conserver | Kafka 4.1, Strimzi (`k8s/kafka`) | perte d'un nœud ou d'une zone : rien de perdu, les écritures continuent |
| Écrire | writers (`python/ecrivain`) | offset validé **après** l'écriture en base : un lot peut être rejoué, jamais perdu ; rejeu sans effet |
| Absorber les pics | KEDA (`k8s/ecrivain/scaledobject.yaml`) | +1 writer par 50 000 messages en attente, jusqu'à 24 ; retour à 2 après 2 min de calme |
| Lire | `vue.<table>` (PostgreSQL principal) | dernier état de chaque ligne, sans doublon ([postgres-principal.md](postgres-principal.md)) |

## Réponses de la passerelle

Contrat complet : [contrat-ecriture.md](contrat-ecriture.md).

| Code | Sens | Que faire |
|------|------|-----------|
| 202 | accepté, rien d'autre à attendre | — |
| 422 | message hors contrat (raison dans la réponse) | corriger, ne pas renvoyer tel quel |
| 503 | Kafka indisponible ou trop lent | renvoyer **le même message** (même id) après `Retry-After` |

## Messages refusés

Un message qui ne peut pas être écrit n'est jamais jeté en silence : il part dans le topic `app-rejets`, avec la raison
(en-tête `raison`) et son origine (`origine` = partition:offset). Raisons possibles : contrat non respecté, clé Kafka
différente de l'id, table inconnue (créer la table avec `lake.creer_table`), données refusées par PostgreSQL (type,
contrainte). Un message refusé par PostgreSQL est isolé : le reste de son lot est écrit normalement.

```bash
kubectl -n spark exec lakehouse-noeuds-0 -- bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic app-rejets --from-beginning --property print.headers=true --max-messages 20
```

## Suivre la charge

```bash
kubectl -n spark exec lakehouse-noeuds-0 -- bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group pg-writer                       # retard (LAG) par partition
kubectl -n spark get scaledobject,hpa ecrivain       # writers voulus par KEDA
```

## Test de charge à 10 000 écritures/s (GKE)

```bash
# table du test
kubectl -n spark exec -it lake-principal-0 -c postgres -- psql -U postgres -d lake \
  -c "SELECT lake.creer_table('charge', 'client text, montant numeric(12,2)')"
# k6 dans le cluster (évite la latence réseau du poste) ; 70 % INSERT, 25 % UPDATE, 5 % DELETE
kubectl -n spark create configmap charge-k6 --from-file=tests/charge/ecritures.js
kubectl -n spark run k6 --rm -i --restart=Never --image=grafana/k6:1.0.0 \
  --overrides='{"spec":{"volumes":[{"name":"s","configMap":{"name":"charge-k6"}}],"containers":[{"name":"k6","image":"grafana/k6:1.0.0","args":["run","-e","URL=http://passerelle:8000","-e","RATE=10000","-e","DUREE=5m","/s/ecritures.js"],"volumeMounts":[{"name":"s","mountPath":"/s"}]}]}}'
```

Le test échoue si le p99 dépasse 500 ms ou si plus de 0,1 % des requêtes échouent. Pendant le test, supprimer un pod
writer, un pod passerelle et un nœud Kafka ; vérifier ensuite `SELECT count(*) FROM vue.charge` et le topic `app-rejets`.

## Mesuré en CI (kind, runner GitHub à 2 cœurs)

`tests/integration/tester_ecritures.py` et k6 à débit réduit, à chaque pull request :

- 3 300 écritures via la passerelle et une rafale de 60 000 messages, avec un writer **tué en plein flux** : aucune
  ligne perdue, aucun doublon, aucune ligne supprimée ressuscitée, historique exact (3 300 sur 3 300) ;
- KEDA : 1 → 4 writers pendant la rafale, retour à 1 au bout de 21 s de calme ;
- k6 à 200 écritures/s pendant 30 s : p99 **123 ms**, 0 % d'échec.

**Le débit de 10 000/s n'a pas encore été mesuré** : il demande le cluster GKE (procédure ci-dessus).
