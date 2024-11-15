import os
import pandas as pd
from glob import glob

# Definir las columnas basadas en las categorías proporcionadas
distress_columns = [
    "30-60-Days_Distress",
    "Absentee",
    "Bankruptcy_Distress",
    "Debt-Collection_Distress",
    "Divorce_Distress",
    "Downsizing_Distress",
    "Estate_Distress",
    "Eviction_Distress",
    "Failed_Listing_Distress",
    "highEquity",
    "Inter_Family_Distress",
    "Judgment_Distress",
    "Lien_City_County_Distress",
    "Lien_HOA_Distress",
    "Lien_Mechanical_Distress",
    "Lien_Other_Distress",
    "Lien_Utility_Distress",
    "Low_income_Distress",
    "PoorCondition_Distress",
    "Preforeclosure_Distress",
    "Probate_Distress",
    "Prop_Vacant_Flag",
    "Senior_Distress",
    "Tax_Delinquent_Distress",
    "Violation_Distress"
]

# Actualizar unique1_columns según tu solicitud
unique1_columns = [
    "MailingFullStreetAddress",
    "MailingZIP5"
]

# Actualizar unique2_columns según tu solicitud
unique2_columns = [
    "SitusFullStreetAddress",
    "SitusZIP5"
]

name_column = ["FIPS"]

key_variables = [
    "LotSizeSqFt",
    "LTV",
    "MailingCity",
    "MailingState",
    "MailingStreet",
    "Owner_Type",
    "Owner1FirstName",
    "Owner1LastName",
    "OwnerNAME1FULL",
    "PropertyID",
    "saleDate",
    "SumLivingAreaSqFt",
    "totalValue",
    "Use_Type",
    "YearBuilt",
    "SitusCity",
    "SitusState"
]

# Combinar todas las columnas a mantener
columns_to_keep = distress_columns + unique1_columns + unique2_columns + name_column + key_variables

# *** Nueva variable para especificar el porcentaje a retener ***
percentage_to_retain = 0.7  # Cambia este valor al porcentaje deseado (entre 0 y 1)

# Validar que el porcentaje esté entre 0 y 1
if not 0 < percentage_to_retain <= 1:
    raise ValueError("El porcentaje a retener debe ser un número entre 0 y 1.")

# Obtener el directorio de trabajo actual
current_dir = os.getcwd()

# Listar todos los directorios en el directorio actual excluyendo 'VM' o 'Virtual Environment'
folders = [f for f in os.listdir(current_dir) if os.path.isdir(f) and f not in ['VM', 'Virtual Environment']]

total_folders = len(folders)
print(f"Se encontraron {total_folders} carpetas para procesar.\n")

for idx, folder in enumerate(folders, 1):
    print(f"Procesando carpeta {idx}/{total_folders}: '{folder}'")
    folder_path = os.path.join(current_dir, folder)

    # Encontrar todos los archivos CSV en la carpeta
    csv_files = glob(os.path.join(folder_path, '*.csv'))
    if not csv_files:
        print(f"No se encontraron archivos CSV en '{folder}'. Saltando.\n")
        continue

    print(f"  Se encontraron {len(csv_files)} archivo(s) CSV en '{folder}'. Leyendo y consolidando...")

    # Leer y concatenar todos los archivos CSV
    dfs = []
    for file in csv_files:
        try:
            df = pd.read_csv(file, low_memory=False)
            dfs.append(df)
        except Exception as e:
            print(f"    Error al leer '{file}': {e}")
    if not dfs:
        print(f"  No se pudieron leer dataframes en '{folder}'. Saltando.\n")
        continue

    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"  Consolidado {len(dfs)} archivo(s) en un dataframe con {combined_df.shape[0]} filas.")

    # Mantener solo las columnas especificadas
    existing_columns = [col for col in columns_to_keep if col in combined_df.columns]
    combined_df = combined_df[existing_columns]
    print(f"  Se retuvieron {len(existing_columns)} columnas según lo especificado.")

    # Crear la variable DistressCounter
    distress_existing = [col for col in distress_columns if col in combined_df.columns]
    distress_df = combined_df[distress_existing].fillna(0)

    # Contar el número de indicadores de distress que son no cero o no vacíos
    distress_counter = distress_df.apply(lambda row: row.astype(bool).sum(), axis=1)
    combined_df['DistressCounter'] = distress_counter
    print("  Se creó la variable 'DistressCounter'.")

    # Eliminar duplicados basados en las columnas UNIQUE1
    unique1_existing = [col for col in unique1_columns if col in combined_df.columns]
    if unique1_existing:
        before_rows = combined_df.shape[0]
        combined_df.drop_duplicates(subset=unique1_existing, inplace=True)
        after_rows = combined_df.shape[0]
        print(f"  Se eliminaron duplicados basados en las columnas UNIQUE1: {before_rows - after_rows} duplicados eliminados.")

    # Eliminar duplicados basados en las columnas UNIQUE2
    unique2_existing = [col for col in unique2_columns if col in combined_df.columns]
    if unique2_existing:
        before_rows = combined_df.shape[0]
        combined_df.drop_duplicates(subset=unique2_existing, inplace=True)
        after_rows = combined_df.shape[0]
        print(f"  Se eliminaron duplicados basados en las columnas UNIQUE2: {before_rows - after_rows} duplicados eliminados.")

    # Ordenar el DataFrame por DistressCounter en orden descendente
    combined_df.sort_values(by='DistressCounter', ascending=False, inplace=True)
    print("  DataFrame ordenado por 'DistressCounter' en orden descendente.")

    # *** Nueva sección: Remover filas basado en porcentaje especificado ***
    total_rows = combined_df.shape[0]
    cutoff_index = max(1, int(total_rows * percentage_to_retain))  # Asegurar al menos una fila
    combined_df = combined_df.iloc[:cutoff_index]
    print(f"  Se retuvo el {percentage_to_retain*100}% superior de filas basado en 'DistressCounter'. Filas reducidas de {total_rows} a {combined_df.shape[0]}.")

    # Guardar el DataFrame en un archivo CSV con el nombre de la carpeta
    output_file = os.path.join(current_dir, f"{folder}.csv")
    combined_df.to_csv(output_file, index=False)
    print(f"  Datos consolidados guardados en '{output_file}'.\n")

print("Procesamiento completo.")