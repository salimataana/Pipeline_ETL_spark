cc🧩 Projet ETL Modulaire et Standard avec PySpark

Ce projet est une implémentation d'un pipeline ETL (Extraction, Transformation, Chargement) modulaire utilisant PySpark. Il permet de traiter des données en plusieurs étapes indépendantes et testées.

🎯 Objectif
- Extraire les données depuis des sources variées (CSV, bases de données, etc)
- Transformer les données avec PySpark (nettoyage, traitements de valeurs manquantes, enrichissement)
- Charger les données transformées dans des formats optimisés (CSV, JSON, etc.)

📁 Structure du projet

.
├── cores/
│   ├── step.py                 # Classe abstraite Step
│   ├── extract_csv.py          # Extraction depuis CSV
│   ├── extract_bd.py           # Extraction depuis base de données
│   ├── transform_main_csv.py   # Transformation des données CSV
│   ├── transform_main_bd.py    # Transformation des données BD
│   ├── load_bd.py              # Chargement format base
│   ├── load_csv.py             # Chargement format fichier
│   └── utils.py                # Utilitaires et FileType
├── main.py                     # Point d'entrée principal
├── pipeline.py                 # Orchestration du pipeline
├── tests/
│   ├── test_extract_bd.py
│   ├── test_extract_csv.py
│   ├── test_transform_bd.py
│   ├── test_transform_bd_csv.py
│   ├── test_load_bd.py
│   ├── test_load_bd_csv.py
│   └── test_pipeline.py
└── README.md


🧪 Tests
Chaque étape du pipeline est testée avec unittest pour garantir la fiabilité des traitements Spark.

🚀 Lancement
Exécution via `main.py` qui orchestre l'enchaînement des étapes ETL avec gestion des sessions Spark.

📌 À propos
Développé par Ana Salimata Sanou pour maîtriser la construction de pipelines data scalables avec PySpark et les bonnes pratiques de génie logiciel.