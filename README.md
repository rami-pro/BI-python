# **Projet ETL : Analyse Socio-Économique**

## **Présentation**

Ce projet vise à extraire, transformer et charger (ETL) des données socio-économiques provenant de fichiers CSV dans un Data Warehouse structuré selon un modèle étoile. L'objectif est de préparer les données pour une analyse avancée avec des outils comme Power BI ou Tableau.

Les données analysées incluent :

- L'indice de développement humain (HDI).
- Les émissions de CO₂ par habitant.
- Le PIB par habitant.
- Les sources d'énergie principales utilisées par chaque pays.
- Les catégories de revenus basées sur le PIB.

Le résultat final est un ensemble de fichiers CSV transformés, prêts à être chargés dans une base de données SQL pour la création d'un Data Warehouse.

---

## **Structure du Projet**

Le projet est organisé comme suit :

```
BI-python/
├── data/
│   ├── input/               # Fichiers CSV bruts
│   └── output/              # Fichiers CSV transformés
├── src/
│   ├── __init__.py          # Marque le répertoire 'src' comme package Python
│   ├── config.py            # Configuration des chemins et seuils
│   ├── extractor.py         # Extraction des données
│   ├── transformer.py       # Transformation des données
│   └── loader.py            # Chargement des CSV finaux
├── requirements.txt         # Liste des dépendances Python
└── run_etl.py               # Point d'entrée du script
```

---

## **Prérequis**

Avant d'exécuter le projet, assurez-vous d'avoir installé les éléments suivants :

1. **Python 3.8+** : Le projet est développé en Python.

Installez les dépendances avec la commande suivante :

```bash
pip install -r requirements.txt
```

---

## **Installation**

1. Clonez le dépôt :

   ```bash
   git clone https://github.com/votre-depot/etl-socioeconomic.git
   cd etl-socioeconomic
   ```

2. Installez les dépendances :

   ```bash
   pip install -r requirements.txt
   ```

---

## **Utilisation**

Pour exécuter l'ETL, suivez ces étapes :

### **1. Préparation des données**

Ajoutez les fichiers CSV bruts dans le dossier `data/input/`. Assurez-vous que les noms des fichiers correspondent aux attentes du script :

- `human-development-index.csv`
- `electricity-prod-source-stacked.csv`
- `gdp-per-capita-worldbank.csv`
- `co-emissions-per-capita.csv`

### **2. Exécution du script**

Exécutez le script principal :

```bash
python run_etl.py
```

### **3. Résultats**

Les fichiers transformés seront générés dans le dossier `data/output/` :

- `Dimension_Pays.csv`
- `Dimension_Temps.csv`
- `Dimension_Energy.csv`
- `Dimension_Developpement_Humain.csv`
- `Dimension_Revenus.csv`
- `Fait_SocioEconomique.csv`

Ces fichiers sont prêts à être chargés dans votre Data Warehouse SQL.

---

## **Détails Techniques**

### **1. Configuration**

Les paramètres globaux (chemins, seuils HDI, catégories de revenus) sont définis dans `config.py`. Vous pouvez modifier ces paramètres si nécessaire.

#### Exemple de configuration :

```python
HDI_THRESHOLDS = {
    "Très élevé": 0.800,
    "Élevé": 0.700,
    "Moyen": 0.550,
    "Faible": 0.000,
}

INCOME_CATEGORIES = {
    "riche": (20000, float("inf")),
    "moyen sup": (10000, 20000),
    "moyen inférieur": (5000, 10000),
    "pauvres": (0, 5000),
}
```

### **2. Étapes du pipeline ETL**

Le pipeline se divise en trois étapes principales :

#### **A. Extraction**

Les données brutes sont extraites des fichiers CSV dans le dossier `data/input/`. Cette étape est gérée par la classe `DataExtractor` dans `extractor.py`.

#### **B. Transformation**

Les données sont nettoyées, transformées et enrichies :

- Catégorisation de l'HDI (`categorize_hdi`).
- Détermination de l'énergie principale (`determine_primary_energy`).
- Catégorisation des revenus (`categorize_income`).
- Filtrage des émissions de CO₂ pour les années ≥ 1990 (`filter_co2`).
- Création des dimensions et jointures pour générer la table de faits.

Cette étape est gérée par la classe `DataTransformer` dans `transformer.py`.

#### **C. Chargement**

Les données transformées sont sauvegardées dans des fichiers CSV dans le dossier `data/output/`. Cette étape est gérée par la classe `DataLoader` dans `loader.py`.

---

## **Modèle Étoile**

Le modèle étoile est composé des tables suivantes :
![schémas en étoile](/assets/modele_etoile.png)
