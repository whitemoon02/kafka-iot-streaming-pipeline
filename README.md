# 🏭 Kafka IoT Streaming Pipeline — supervision de cuves de fermentation en temps réel

Pipeline de données **temps réel** qui collecte les mesures de capteurs industriels (température, pH,
pression, oxygène dissous) sur 6 cuves de fermentation, les fiabilise, détecte les anomalies et stocke
des agrégats exploitables dans PostgreSQL / Grafana.

## 🧱 Architecture

```
 ┌──────────────┐   sensor-readings    ┌─────────────────────────────┐      ┌────────────┐
 │  Producteur  │ ───────────────────► │        Consommateur         │ ───► │ PostgreSQL │ ──► Grafana
 │ (6 cuves × 4 │  Kafka (3 partitions,│ validation → dédoublonnage  │      │ agrégats,  │
 │   capteurs)  │   clé = cuve)        │ → z-score → fenêtres 1 min  │      │ anomalies  │
 └──────────────┘                      └──────────────┬──────────────┘      └────────────┘
                                                      │ rejets
                                                      ▼
                                            sensor-readings-dlq
```

## ✨ Points techniques

| Sujet | Choix |
|---|---|
| Ordonnancement | Clé de partition = `tank_id` → ordre garanti par cuve |
| Fiabilité producteur | `acks=all`, producteur idempotent, compression lz4 |
| Garantie de livraison | *At-least-once* : commit manuel des offsets **après** écriture en base, puis dédoublonnage par `event_id` |
| Qualité des données | Validation du schéma, des types, des plages physiques et des horodatages ; rejets vers une **dead letter queue** |
| Détection d'anomalies | z-score sur fenêtre glissante par cuve et par mesure, sans apprentissage sur les points anormaux |
| Agrégation | Fenêtres fixes d'une minute (count, moyenne, min, max) |
| Testabilité | Toute la logique métier (`app/processing.py`) est indépendante de Kafka et couverte par des tests |

## 🚀 Lancer le projet

**Avec Docker (stack complète)**

```bash
docker compose up --build
# Grafana : http://localhost:3000 (admin/admin) → source PostgreSQL : postgres:5432, iot/iot
```

**Sans Docker (démo locale, SQLite)** — simule 2 h de flux en quelques secondes :

```bash
python -m app.run_local --minutes 120
pytest
```

Exemple de sortie :

```
Statistiques du flux : {'received': 34593, 'valid': 34408, 'rejected': 152, 'anomalies': 343, 'duplicates': 33}

Motifs de rejet :
   ('hors plage physique [0.0, 14.0]', 22)
   ('valeur non numérique', 70)
   ...
```

## 📊 Requêtes utiles

```sql
-- Température moyenne par cuve sur la dernière heure
SELECT tank_id, ROUND(AVG(avg)::numeric, 2) FROM sensor_aggregates
WHERE metric = 'temperature' GROUP BY tank_id;

-- Cuves avec le plus d'anomalies
SELECT tank_id, metric, COUNT(*) FROM anomalies GROUP BY 1, 2 ORDER BY 3 DESC;
```

## 🛠️ Stack

Python · Apache Kafka (KRaft) · PostgreSQL · Docker Compose · Grafana · Pytest

## 🔭 Pistes d'évolution

- Schema Registry + Avro pour versionner le format des messages
- Kafka Streams / Flink pour le fenêtrage distribué
- Alerting Grafana sur la table `anomalies`

---
👤 **Espérance DAYE** — Data Engineer · [LinkedIn](https://linkedin.com/in/Esperance-DAYE)
