import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import shutil  # For creating/clearing the output directory

# Load environment variables
load_dotenv()

# Database connection
engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@localhost:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

def extract():
    # Read CSV files (no changes here)
    df_vaccine_india = pd.read_csv("vaccnation-1year-india.csv")
    df_vaccine_brazil = pd.read_csv("vaccnation-1year-brazil.csv")
    df_mortality = pd.read_csv("child-mortality.csv")
    return df_vaccine_india, df_vaccine_brazil, df_mortality

def transform(df_vaccine_india, df_vaccine_brazil, df_mortality):
    # Combine, clean, and merge data (no changes here)
    df_vaccines = pd.concat([df_vaccine_india, df_vaccine_brazil], ignore_index=True)
    df_vaccines = df_vaccines.rename(columns={
        "Number of one-year-olds vaccinated with three doses of Hepatitis B containing vaccine (HEPB3)": "HEPB3",
        "Number of one-year-olds vaccinated with three doses of combined diphtheria, tetanus toxoid and pertussis- containing vaccine (DTP3)": "DTP3",
        "Number of one-year-olds vaccinated with the third dose of either oral or inactivated polio vaccine (POL3)": "POL3",
        "Population - Sex: all - Age: 0 - Variant: estimates": "population",
        "Number of one-year-olds vaccinated with the first dose of measles-containing vaccine (MCV1)" : "MCV1",
        "Number of one-year-olds vaccinated with three doses of Haemophilus influenzae type b containing vaccine (HIB3)" : "HIB3",
        "Number of one-year-olds vaccinated with one dose of rubella-containing vaccine (RCV1)" : "RCV1",
        "Number of one-year-olds vaccinated with the final recommended dose (2nd or 3rd) of rotavirus vaccine (ROTAC)" : "ROTAC"
    })
    df_mortality = df_mortality.rename(columns={"Under-five mortality rate": "mortality_rate"})
    
    merged_df = pd.merge(
        df_vaccines,
        df_mortality,
        on=["Entity", "Code", "Year"],
        how="left"
    )
    
    # Create dimension tables
    dim_country = merged_df[["Entity", "Code"]].drop_duplicates().reset_index(drop=True)
    dim_country["country_id"] = dim_country.index + 1
    
    dim_year = merged_df[["Year"]].drop_duplicates().reset_index(drop=True)
    dim_year["year_id"] = dim_year.index + 1
    
    # Prepare fact table
    fact_df = merged_df.merge(dim_country, on=["Entity", "Code"])
    fact_df = fact_df.merge(dim_year, on=["Year"])
    fact_df = fact_df[[
        "country_id",
        "year_id",
        "mortality_rate",
        "HEPB3",
        "DTP3",
        "POL3",
        "MCV1",
        "HIB3",
        "RCV1",
        "ROTAC"
    ]]
    
    return dim_country, dim_year, fact_df

def load(dim_country, dim_year, fact_df):
    # Create output directory (overwrite if exists)
    output_dir = "output"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)
    
    # # Load to PostgreSQL
    # dim_country.to_sql("dim_country", engine, if_exists="replace", index=False)
    # dim_year.to_sql("dim_year", engine, if_exists="replace", index=False)
    # fact_df.to_sql("fact_health_metrics", engine, if_exists="replace", index=False)
    
    # Save as CSV
    dim_country.to_csv(f"{output_dir}/dim_country.csv", index=False)
    dim_year.to_csv(f"{output_dir}/dim_year.csv", index=False)
    fact_df.to_csv(f"{output_dir}/fact_health_metrics.csv", index=False)

if __name__ == "__main__":
    df1, df2, df3 = extract()
    dim_country, dim_year, fact_df = transform(df1, df2, df3)
    load(dim_country, dim_year, fact_df)