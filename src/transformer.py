import pandas as pd
import numpy as np
from src.config import HDI_THRESHOLDS, INCOME_CATEGORIES

class DataTransformer:
    def __init__(self):
        pass

    def categorize_hdi(self, df_hdi):
        """Catégorise l'HDI selon les seuils des Nations Unies."""
        df_hdi = df_hdi.dropna(subset=["Human Development Index"])

        df_hdi["niveau_developpement"] = pd.cut(
            df_hdi["Human Development Index"],
            bins=[HDI_THRESHOLDS["Faible"], HDI_THRESHOLDS["Moyen"], 
                  HDI_THRESHOLDS["Élevé"], HDI_THRESHOLDS["Très élevé"], float("inf")],
            labels=["Faible", "Moyen", "Élevé", "Très élevé"],
            include_lowest=True
        )
        return df_hdi

    def determine_primary_energy(self, df_energy):
        """Détermine l'énergie principale pour chaque pays/année."""
        energy_columns = [
            "Electricity from coal - TWh",
            "Electricity from oil - TWh",
            "Electricity from gas - TWh",
            "Electricity from nuclear - TWh",
            "Electricity from hydro - TWh",
            "Electricity from solar - TWh",
            "Electricity from wind - TWh",
            "Other renewables excluding bioenergy - TWh"
        ]
        # Drop rows where all energy columns are missing
        df_energy = df_energy.dropna(subset=energy_columns, how="all")
        
        df_energy["primary_energy"] = df_energy[energy_columns].idxmax(axis=1)
        df_energy["energie_principale"] = np.where(
            df_energy["primary_energy"].str.contains("coal|oil|gas"),
            "fossil",
            np.where(
                df_energy["primary_energy"].str.contains("nuclear"),
                "nuclear",
                np.where(
                    df_energy["primary_energy"].str.contains("hydro|solar|wind|bioenergy"),
                    "renouvelable",
                    "mix"
                )
            )
        )
        return df_energy[["Entity", "Code", "Year", "energie_principale"]]
    
    def categorize_income(self, df_gdp):
        """Catégorise les revenus en fonction du PIB par habitant."""
        
        # Drop rows with missing GDP per capita values
        df_gdp = df_gdp.dropna(subset=["GDP per capita"])
        
        # Extract and sort the lower bounds of each income category
        bins = sorted([v[0] for v in INCOME_CATEGORIES.values()])
        bins.append(float("inf"))  # Add the upper bound

        # Use pd.cut() with the sorted bins
        df_gdp["categorie_revenu"] = pd.cut(
            df_gdp["GDP per capita"],
            bins=bins,
            labels=INCOME_CATEGORIES.keys(),
            right=False
        )
        return df_gdp

    def filter_co2(self, df_co2):
        """Filtre les données de CO₂ pour les années ≥ 1990."""
        # Drop rows with missing CO₂ values
        df_co2 = df_co2.dropna(subset=["Annual CO₂ emissions (per capita)"])
        
        return df_co2[df_co2["Year"] >= 1990]

    def create_dimensions(self, df_hdi, df_energy, df_gdp, df_co2):
        """Crée les dimensions et la table de faits."""
        # Dimension Pays
        pays = df_hdi[["Entity", "Code"]].drop_duplicates()
        pays["ID_Pays"] = range(1, len(pays) + 1)
        
        # Dimension Temps
        annees = df_hdi[["Year"]].drop_duplicates()
        annees["ID_Annee"] = range(1, len(annees) + 1)
        
        # Dimension Energy
        energy = df_energy[["energie_principale"]].drop_duplicates()
        energy["ID_energie_principale"] = range(1, len(energy) + 1)
        
        # Dimension Développement Humain
        dev_humain = df_hdi[["niveau_developpement"]].drop_duplicates()
        dev_humain["ID_niveau_developpement"] = range(1, len(dev_humain) + 1)
        
        # Dimension Revenus
        revenus = df_gdp[["categorie_revenu"]].drop_duplicates()
        revenus["ID_Revenu"] = range(1, len(revenus) + 1)
        
        return {
            "pays": pays,
            "temps": annees,
            "energy": energy,
            "dev_humain": dev_humain,
            "revenus": revenus,
        }