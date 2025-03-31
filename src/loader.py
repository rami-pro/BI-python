import os
from src.config import DATA_OUTPUT_DIR

class DataLoader:
    def __init__(self):
        self.output_dir = DATA_OUTPUT_DIR

    def save_to_csv(self, df, filename):
        """Sauvegarde un DataFrame dans un fichier CSV."""
        df.to_csv(os.path.join(self.output_dir, filename), index=False)

    def load_all(self, dimensions, df_faits):
        """Sauvegarde toutes les tables."""
        self.save_to_csv(dimensions["pays"], "Dimension_Pays.csv")
        self.save_to_csv(dimensions["temps"], "Dimension_Temps.csv")
        self.save_to_csv(dimensions["energy"], "Dimension_Energy.csv")
        self.save_to_csv(dimensions["dev_humain"], "Dimension_Developpement_Humain.csv")
        self.save_to_csv(dimensions["revenus"], "Dimension_Revenus.csv")
        self.save_to_csv(df_faits, "Fait_SocioEconomique.csv")