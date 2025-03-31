from src.extractor import DataExtractor
from src.transformer import DataTransformer
from src.loader import DataLoader
from src.config import INCOME_CATEGORIES
import pandas as pd

def main():
    # Extraction
    extractor = DataExtractor()
    data = extractor.extract_all()
    df_hdi = data["hdi"]
    df_energy = data["energy"]
    df_gdp = data["gdp"]
    df_co2 = data["co2"]

    # Transformation
    transformer = DataTransformer()
    df_hdi = transformer.categorize_hdi(df_hdi)
    df_energy = transformer.determine_primary_energy(df_energy)
    df_gdp = transformer.categorize_income(df_gdp)
    df_co2 = transformer.filter_co2(df_co2)
    dimensions = transformer.create_dimensions(df_hdi, df_energy, df_gdp, df_co2)

    # Fusionner les données
    merged_data = pd.merge(df_hdi, df_co2, on=["Entity", "Code", "Year"], how="inner")
    merged_data = pd.merge(merged_data, df_gdp, on=["Entity", "Code", "Year"], how="inner")
    merged_data = pd.merge(merged_data, df_energy, on=["Entity", "Code", "Year"], how="inner")

    # Mapper les dimensions
    merged_data = merged_data.merge(
        dimensions["pays"][["Entity", "Code", "ID_Pays"]],
        on=["Entity", "Code"],
        how="left"
    ).merge(
        dimensions["temps"][["Year", "ID_Annee"]],
        on=["Year"],
        how="left"
    ).merge(
        dimensions["energy"][["energie_principale", "ID_energie_principale"]],
        on=["energie_principale"],
        how="left"
    ).merge(
        dimensions["dev_humain"][["niveau_developpement", "ID_niveau_developpement"]],
        on=["niveau_developpement"],
        how="left"
    )
    
    bins_income = sorted([v[0] for v in INCOME_CATEGORIES.values()])
    bins_income.append(float("inf"))  # Add the upper bound

    # Mapper ID_Revenu
    merged_data["categorie_revenu"] = pd.cut(
        merged_data["GDP per capita"],
        bins=bins_income,
        labels=INCOME_CATEGORIES.keys(),
        right=False
    )
    merged_data = merged_data.merge(
        dimensions["revenus"][["categorie_revenu", "ID_Revenu"]],
        on=["categorie_revenu"],
        how="left"
    )

    # Créer la table de faits
    df_faits = merged_data[[
        "ID_Pays",
        "ID_Annee",
        "ID_Revenu",
        "ID_energie_principale",
        "ID_niveau_developpement",
        "Human Development Index",
        "Annual CO₂ emissions (per capita)",
        "GDP per capita"
    ]].rename(columns={
        "Human Development Index": "HDI",
        "Annual CO₂ emissions (per capita)": "Emissions_CO2_par_habitant",
        "GDP per capita": "PIB_par_habitant"
    }).dropna()

    # Chargement
    loader = DataLoader()
    loader.load_all(dimensions, df_faits)

if __name__ == "__main__":
    main()