import pandas as pd
import os
from src.config import DATA_INPUT_DIR

class DataExtractor:
    def __init__(self):
        self.input_dir = DATA_INPUT_DIR

    def read_csv(self, filename):
        """Lit un fichier CSV et retourne un DataFrame."""
        return pd.read_csv(os.path.join(self.input_dir, filename))

    def extract_all(self):
        """Extrait tous les fichiers CSV nécessaires."""
        return {
            "hdi": self.read_csv("human-development-index.csv"),
            "energy": self.read_csv("electricity-prod-source-stacked.csv"),
            "gdp": self.read_csv("gdp-per-capita-worldbank.csv"),
            "co2": self.read_csv("co-emissions-per-capita.csv"),
        }