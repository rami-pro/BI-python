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
                  HDI_THRESHOLDS["Eleve"], HDI_THRESHOLDS["Tres_eleve"], float("inf")],
            labels=["Faible", "Moyen", "Eleve", "Tres_eleve"],
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
            "Electricity from bioenergy - TWh",
            "Other renewables excluding bioenergy - TWh"
        ]

        # Drop rows where all energy columns are missing
        df_energy = df_energy.dropna(subset=energy_columns, how="all")

        # Step 1: Group energies by category
        df_energy["fossil_energy"] = (
            df_energy["Electricity from coal - TWh"].fillna(0) +
            df_energy["Electricity from oil - TWh"].fillna(0) +
            df_energy["Electricity from gas - TWh"].fillna(0)
        )
        df_energy["nuclear_energy"] = df_energy["Electricity from nuclear - TWh"].fillna(0)
        df_energy["renewable_energy"] = (
            df_energy["Electricity from hydro - TWh"].fillna(0) +
            df_energy["Electricity from solar - TWh"].fillna(0) +
            df_energy["Electricity from wind - TWh"].fillna(0) +
            df_energy["Electricity from bioenergy - TWh"].fillna(0) +
            df_energy["Other renewables excluding bioenergy - TWh"].fillna(0)
        )

        # Step 2: Calculate total energy production
        df_energy["total_energy"] = df_energy[energy_columns].sum(axis=1)

        # Step 3: Determine the dominant energy category
        df_energy["energie_principale"] = "mix"  # Default value
        df_energy["energie_principale"] = df_energy.apply(
        lambda row: (
            "fossil" if row["total_energy"] > 0 and row["fossil_energy"] / row["total_energy"] > 0.5 else
            "nuclear" if row["total_energy"] > 0 and row["nuclear_energy"] / row["total_energy"] > 0.5 else
            "renouvelable" if row["total_energy"] > 0 and row["renewable_energy"] / row["total_energy"] > 0.5 else
            "mix"
            ),
            axis=1
        )

        # Step 4: Return only necessary columns
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