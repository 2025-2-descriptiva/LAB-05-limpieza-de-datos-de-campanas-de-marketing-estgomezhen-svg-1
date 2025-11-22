"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
def clean_campaign_data():
    import os
    import glob
    import zipfile
    import pandas as pd

    input_dir = "files/input"
    output_dir = "files/output"
    #  Leer  los .csv que están dentro de los .zip en files/input
    tablas = []

    for zip_path in glob.glob(os.path.join(input_dir, "*.zip")):
        with zipfile.ZipFile(zip_path) as z:
            # Recorre archivos dentro del zip
            for nombre in z.namelist():
                if nombre.lower().endswith(".csv"):
                    with z.open(nombre) as fh:
                        df_tmp = pd.read_csv(fh)
                        tablas.append(df_tmp)

    if not tablas:
        return

    # Unir todos los CSV en un solo DataFrame
    datos = pd.concat(tablas, ignore_index=True)

    # Limpieza de columnas comunes
    # job: "." -> "" y "-" -> "_"
    datos["job"] = (
        datos["job"]
        .str.replace(".", "", regex=False)
        .str.replace("-", "_", regex=False)
    )
    # education: "." -> "_" y "unknown" -> NA
    datos["education"] = (
        datos["education"]
        .str.replace(".", "_", regex=False)
        .replace("unknown", pd.NA)
    )
    # credit_default y mortgage: yes -> 1, otro -> 0
    datos["credit_default"] = (datos["credit_default"] == "yes").astype(int)
    datos["mortgage"] = (datos["mortgage"] == "yes").astype(int)

    # previous_outcome: success -> 1, otro -> 0
    datos["previous_outcome"] = (datos["previous_outcome"] == "success").astype(int)

    # campaign_outcome: yes -> 1, otro -> 0
    datos["campaign_outcome"] = (datos["campaign_outcome"] == "yes").astype(int)

    # Construir la fecha last_contact_date = "2022-MM-DD"

    codigo_mes = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12",
    }

    mes_str = datos["month"].str.lower().map(codigo_mes)
    dia_str = datos["day"].astype(int).astype(str).str.zfill(2)

    datos["last_contact_date"] = "2022-" + mes_str + "-" + dia_str
    # Crear carpeta de salida
    os.makedirs(output_dir, exist_ok=True)
    # client.csv
    columnas_cliente = [
        "client_id",
        "age",
        "job",
        "marital",
        "education",
        "credit_default",
        "mortgage",
    ]
    df_client = datos[columnas_cliente].copy()
    df_client.to_csv(os.path.join(output_dir, "client.csv"), index=False)
  
    columnas_campaña = [
        "client_id",
        "number_contacts",
        "contact_duration",
        "previous_campaign_contacts",
        "previous_outcome",
        "campaign_outcome",
        "last_contact_date",
    ]
    df_campaign = datos[columnas_campaña].copy()
    df_campaign.to_csv(os.path.join(output_dir, "campaign.csv"), index=False)

    columnas_econ = [
        "client_id",
        "cons_price_idx",
        "euribor_three_months",
    ]
    df_econ = datos[columnas_econ].copy()
    df_econ.to_csv(os.path.join(output_dir, "economics.csv"), index=False)

    return
