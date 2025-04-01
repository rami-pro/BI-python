import os

# Chemins des fichiers
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_INPUT_DIR = os.path.join(BASE_DIR, "data", "input")
DATA_OUTPUT_DIR = os.path.join(BASE_DIR, "data", "output")

# Seuils HDI (Nations Unies)
HDI_THRESHOLDS = {
    "Tres_eleve": 0.800,
    "Eleve": 0.700,
    "Moyen": 0.550,
    "Faible": 0.000,
}

# Catégories de revenus (basées sur le PIB par habitant)
INCOME_CATEGORIES = {
    "riche": (20000, float("inf")),
    "moyen_sup": (10000, 20000),
    "moyen_inferieur": (5000, 10000),
    "pauvres": (0, 5000),
}