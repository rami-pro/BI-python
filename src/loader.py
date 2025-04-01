import os
from src.config import DATA_OUTPUT_DIR
from dotenv import load_dotenv
import psycopg2
import pandas as pd
import chardet


class DataLoader:
    def __init__(self):
        self.output_dir = DATA_OUTPUT_DIR

    def save_to_csv(self, df, filename):
        """Sauvegarde un DataFrame dans un fichier CSV."""
        df.to_csv(os.path.join(self.output_dir, filename), index=False, encoding="utf-8")

    def load_all(self, dimensions, df_faits):
        """Sauvegarde toutes les tables."""
        self.save_to_csv(dimensions["pays"], "Dimension_Pays.csv".lower())
        self.save_to_csv(dimensions["temps"], "Dimension_Temps.csv".lower())
        self.save_to_csv(dimensions["energy"], "Dimension_Energy.csv".lower())
        self.save_to_csv(dimensions["dev_humain"], "Dimension_Developpement_Humain.csv".lower())
        self.save_to_csv(dimensions["revenus"], "Dimension_Revenus.csv".lower())
        self.save_to_csv(df_faits, "Fait_SocioEconomique.csv".lower())
        
class DatabaseLoader:
    def __init__(self):
        # Charger les variables d'environnement depuis .env
        load_dotenv()
        self.conn = None
        self.db_config = {
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
        }

    def connect(self):
        """Établit une connexion à la base de données PostgreSQL."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            print("Connexion à la base de données réussie.")
        except Exception as e:
            print(f"Erreur de connexion à la base de données : {e}")

    def create_tables(self):
        """Crée les tables nécessaires dans la base de données."""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS Dimension_Pays (
                ID_Pays SERIAL PRIMARY KEY,
                Entity VARCHAR(255) NOT NULL,
                Code VARCHAR(255) NOT NULL

            );
            """,
            """
            CREATE TABLE IF NOT EXISTS Dimension_Temps (
                ID_Annee SERIAL PRIMARY KEY,
                Year INTEGER NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS Dimension_Energy (
                ID_energie_principale SERIAL PRIMARY KEY,
                energie_principale VARCHAR(255) NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS Dimension_Developpement_Humain (
                ID_niveau_developpement SERIAL PRIMARY KEY,
                niveau_developpement VARCHAR(255) NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS Dimension_Revenus (
                ID_Revenu SERIAL PRIMARY KEY,
                categorie_revenu VARCHAR(255) NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS Fait_SocioEconomique (
                ID_Pays INTEGER REFERENCES Dimension_Pays(ID_Pays),
                ID_Annee INTEGER REFERENCES Dimension_Temps(ID_Annee),
                ID_Revenu INTEGER REFERENCES Dimension_Revenus(ID_Revenu),
                ID_energie_principale INTEGER REFERENCES Dimension_Energy(ID_energie_principale),
                ID_niveau_developpement INTEGER REFERENCES Dimension_Developpement_Humain(ID_niveau_developpement),
                HDI FLOAT,
                Emissions_CO2_par_habitant FLOAT,
                PIB_par_habitant FLOAT,
                PRIMARY KEY (ID_Pays, ID_Annee, ID_Revenu, ID_energie_principale, ID_niveau_developpement)
            );
            """
        ]

        with self.conn.cursor() as cursor:
            for query in queries:
                cursor.execute(query)
            self.conn.commit()
        print("Tables créées avec succès.")

    def truncate_table(self, table_name):
        """Vide une table existante."""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
            self.conn.commit()
            print(f"Table {table_name} vidée avec succès.")
        except Exception as e:
            print(f"Erreur lors du vidage de la table {table_name} : {e}")

    # def load_csv_to_table(self, file_path, table_name):
    #     """Charge un fichier CSV dans une table PostgreSQL."""
    #     try:
    #         with self.conn.cursor() as cursor:
    #             with open(file_path, "r", encoding="utf-8") as f:
    #                 next(f)  # Ignorer l'en-tête
    #                 cursor.copy_from(f, table_name, sep=",")
    #         self.conn.commit()
    #         print(f"Données chargées dans la table {table_name}.")
    #     except Exception as e:
    #         print(f"Erreur lors du chargement des données dans {table_name} : {e}")
    
    def load_csv_to_table(self, file_path, table_name):
        """Charge un fichier CSV dans une table PostgreSQL."""
        try:
            # Vérifier si le fichier existe
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Fichier {file_path} introuvable.")
            
            # Détecter l'encodage du fichier
            with open(file_path, "rb") as f:
                result = chardet.detect(f.read())
                encoding = result["encoding"]
            print(encoding)

            # Lire le fichier CSV avec Pandas
            try:
                df = pd.read_csv(file_path, encoding=encoding)
            except Exception as e:
                print(f"Erreur d'encodage : {e}. Encodage détecté : {encoding}")
                raise
            # print(df.head(1))
            
            # Lire le fichier CSV avec Pandas
            df.columns = df.columns.str.strip().str.lower()

            # Réorganiser les colonnes selon l'ordre attendu par PostgreSQL
            expected_columns = {
                "dimension_pays": ["id_pays", "entity", "code"],
                "dimension_temps": ["id_annee", "year"],
                "dimension_energy": ["id_energie_principale", "energie_principale"],
                "dimension_developpement_humain": ["id_niveau_developpement", "niveau_developpement"],
                "dimension_revenus": ["id_revenu", "categorie_revenu"],
                "fait_socioeconomique": [
                    "id_pays",
                    "id_annee",
                    "id_revenu",
                    "id_energie_principale",
                    "id_niveau_developpement",
                    "hdi",
                    "emissions_co2_par_habitant",
                    "pib_par_habitant"
                ]
            }
            
            # print("Colonnes attendues :", expected_columns[table_name])
            # print("Colonnes dans le fichier CSV :", list(df.columns))
            # # Vérifier que toutes les colonnes attendues sont présentes dans le fichier CSV
            missing_columns = [col for col in expected_columns[table_name] if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Colonnes manquantes dans le fichier CSV : {missing_columns}")


            # Réorganiser les colonnes
            df = df[expected_columns[table_name.lower()]]

            # Enregistrer le fichier temporaire avec l'ordre correct
            temp_file_path = file_path + ".temp"
            df.to_csv(temp_file_path, index=False)

            # Charger le fichier temporaire dans PostgreSQL
            with self.conn.cursor() as cursor:
                with open(temp_file_path, "r", encoding="utf-8") as f:
                    next(f)  # Ignorer l'en-tête
                    cursor.copy_from(f, table_name, sep=",")
            self.conn.commit()
            print(f"Données chargées dans la table {table_name}.")
            
            # Supprimer le fichier temporaire
            os.remove(temp_file_path)
        except FileNotFoundError as e:
            print(f"Erreur lors du chargement des données dans {table_name} : {e}")
        except FileNotFoundError as e:
            print(f"Erreur lors du chargement des données dans zzzz {table_name} : {e}")
        except Exception as e:
            print(f"Erreur lors du chargement des données dans eeee {table_name} : {e}")


    def close(self):
        """Ferme la connexion à la base de données."""
        if self.conn:
            self.conn.close()
            print("Connexion fermée.")