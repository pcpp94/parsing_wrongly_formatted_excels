import os
import re
import pandas as pd
import numpy as np
from lxml import etree
import datetime

from .config import BASE_DIR

main_dir = os.path.abspath(os.path.join(BASE_DIR, ".."))
project_dir = BASE_DIR

hacienda = [
    "Hacienda1",
    "Hacienda2",
    "Hacienda3",
    "Hacienda4",
    "Hacienda5",
    "Hacienda6",
    "Hacienda7",
    "Hacienda8",
]
variable_symb = [
    "SS2_12",
    "SX2_12",
    "SX2_13",
    "SX2_14",
    "SC_022",
    "CUZ02",
    "PUNO_02",
    "AQP_S23",
]
directory = "C:\\Users\\pcpp94\\\Documents\\southern_2"
black_list = ["2023-03_southern_2_KWH_Consumption_Hourly_V1.xlsx"]
max_header_length = 22


def open_badly_saved_excel(file_path):
    """
    Function to open the Excel files that are INCORRECTLY saved as Excels - they are XMLs.

    Args:
        file_path (str): file path

    Returns:
        df: pd.DataFrame
    """

    xml_file_path = file_path
    xlsx_file_path = file_path.replace(".xls", ".xlsx")

    try:
        # Parse the XML Spreadsheet 2003 file
        with open(xml_file_path, "r", encoding="utf-8") as file:
            xml_content = file.read()
        # Use lxml to parse the XML content
        tree = etree.fromstring(xml_content)
    except:
        # Parse the XML Spreadsheet 2003 file
        with open(xml_file_path, "rb") as file:
            xml_content = file.read()
        # Use lxml to parse the XML content
        tree = etree.fromstring(xml_content)

    # Define namespaces
    namespaces = {"ss": "urn:schemas-microsoft-com:office:spreadsheet"}

    # Extract data from XML
    data = []
    max_col = 0
    row_index = 0

    for row in tree.xpath("//ss:Row", namespaces=namespaces):
        row_data = []
        col_index = 0
        for cell in row.xpath("ss:Cell", namespaces=namespaces):
            # Handle cell indexing
            cell_index = int(cell.attrib.get("ss:Index", col_index + 1)) - 1
            while col_index < cell_index:
                row_data.append("")
                col_index += 1

            # Get the cell value
            cell_value = cell.xpath("ss:Data", namespaces=namespaces)
            cell_text = cell_value[0].text if cell_value else ""

            # Handle merged cells
            merge_across = int(cell.attrib.get("ss:MergeAcross", 0))
            merge_down = int(cell.attrib.get("ss:MergeDown", 0))

            # Add the cell text to the current row
            row_data.append(cell_text)
            col_index += 1

            # Extend the row data for merged cells across
            if merge_across > 0:
                for _ in range(merge_across):
                    row_data.append("")
                    col_index += 1
            max_col = max(max_col, col_index)

            # Handle merged cells down
            for i in range(1, merge_down + 1):
                target_row_index = row_index + i
                # Ensure the target row exists
                while len(data) <= target_row_index:
                    data.append([""] * max_col)
                # Fill the merged cell text in the target row
                data[target_row_index][col_index - 1] = cell_text

        data.append(row_data)
        row_index += 1

    # Normalize the row lengths
    for row in data:
        while len(row) < max_col:
            row.append("")

    # Convert data to a DataFrame
    df = pd.DataFrame(data)

    df = df.applymap(lambda x: np.nan if x == "" else x)

    return df


def infer_series_dtype(series):
    """
    Function to infer the dtypes of the pandas dataframes columns

    Args:
        series (pd.Series): Column of a Pandas DataFrame

    Returns:
        (str): String stating the type of the series after being parsed.
    """

    # Numeric test
    try:
        pd.to_numeric(series.dropna())
        return "numeric"
    except:
        pass
    # Datetime test
    try:
        pd.to_datetime(series.dropna())
        return "datetime"
    except:
        pass
    return "string"


def drop_sum_column(df):
    """
    Drop column if it is the sum of the variables.

    Args:
        df (pd.DataFrame): Pandas DataFrame

    Returns:
        df (pd.DataFrame): Pandas DataFrame
    """
    df2 = df.select_dtypes("float")
    df2 = df2.iloc[:-5, :]
    for col in df2.columns:
        # Sum of all other columns

        other_cols = df2.drop(columns=[col])
        sum_of_others = other_cols.sum(axis=1)

        # Check if the current column is the ~sum of the others
        if all(df2[col].round(0) == sum_of_others.round(0)):
            df = df.drop(columns=col)

    return df


def get_last_modified_filelist(directory, black_list):
    """
    Get the list of the southern_2 Files with the date they were lastly modified.
    Also include list of ad-hoc files we do not wish to include.

    Args:
        directory (string): Path to southern_2 Folder
        black_list (list): List of filenames that we do not wish to parse.

    Returns:
        modified_date_files (pd.DataFrame) : pandas DataFrame containing a column with modified datetime.
    """
    # Directory containing the .xlsx files

    direc = os.listdir(directory)
    direc = [x for x in direc if x not in black_list]
    modified_date_files = pd.DataFrame({"excel filename": direc})
    for index, row in zip(
        modified_date_files.index, modified_date_files["excel filename"].values
    ):
        modified_date_files.loc[index, "modified_date"] = os.path.getmtime(
            os.path.join(directory, row)
        )

    return modified_date_files


def parse_all_files(directory, black_list):
    """
    Parse all southern_2 Files from the input directory

    Args:
        directory (str): Path to the directory containing the southern_2 files.

    Returns:
        all_dataframes (list): List containing all the Parsed Pandas DataFrames
    """

    # List to hold all the dataframes
    all_dataframes = []

    direc = os.listdir(directory)
    direc = [x for x in direc if x not in black_list]
    # Iterate over each file in the directory
    for filename in direc:
        if filename.endswith((".xlsx", ".xls")):

            # Construct the full file path
            file_path = os.path.join(directory, filename)

            # Read each sheet in the Excel file
            try:
                if filename.split(".")[-1] == "xls":
                    xls = pd.ExcelFile(file_path, engine="xlrd")
                    sheet_names = xls.sheet_names
                else:
                    xls = pd.ExcelFile(file_path, engine="openpyxl")
                    sheet_names = xls.sheet_names
            except:
                df = open_badly_saved_excel(file_path)
                sheet_names = ["XML_file"]

            for sheet_name in sheet_names:

                # Read the sheet into a dataframe
                if sheet_name == "XML_file":
                    pass
                else:
                    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

                # 1. Get the number of rows in the dataframe
                num_rows = df.shape[0]
                num_cols = df.shape[1]

                # 2. Get rid of rows with 2< values
                threshold = 1
                df.dropna(axis=0, thresh=threshold, inplace=True)

                # 3. Get rid of columns with 90% of NA values
                threshold = 0.3 * num_rows  # 90% threshold for dropping columns
                df.dropna(axis=1, thresh=threshold, inplace=True)

                # 4. Get what type of column is each one by getting the 90%
                indeces_list = []
                df_corrected = pd.DataFrame()

                for col in df.columns:

                    type = infer_series_dtype(
                        df.loc[num_rows * 0.1 : num_rows * 0.9, col]
                    )

                    if type == "datetime":
                        indeces = (
                            pd.to_datetime(df[col], errors="coerce")
                            .loc[: num_rows * 0.9][
                                pd.to_datetime(df[col].fillna(0), errors="coerce")
                                .loc[: num_rows * 0.9]
                                .isna()
                            ]
                            .index
                        )
                        indeces_list.append(indeces.to_list())
                    elif type == "numeric":
                        indeces = (
                            pd.to_numeric(df[col], errors="coerce")
                            .loc[: num_rows * 0.9][
                                pd.to_numeric(df[col].fillna(0), errors="coerce")
                                .loc[: num_rows * 0.9]
                                .isna()
                            ]
                            .index
                        )
                        indeces_list.append(indeces.to_list())
                    else:
                        continue

                indeces = list(range(0, max(indeces_list)[0] + 1))

                pattern = r"(Crypt.*?)/"

                prev_name = []
                datetime_name = []
                # 5. Cleaning and headers.
                for col in df.columns:

                    type = infer_series_dtype(
                        df.loc[num_rows * 0.1 : num_rows * 0.9, col]
                    )

                    if type == "datetime":
                        headers = df.loc[indeces, col].dropna()
                        headers = headers.apply(
                            lambda x: (
                                re.search(pattern, x).group(1)
                                if re.search(pattern, x) != None
                                else x
                            )
                        )
                        if len(headers) == 0:
                            header = "missing"
                        else:
                            header = "|".join(
                                str(item)
                                for item in headers.dropna().values
                                if len(item) < max_header_length
                            )
                            datetime_name.append(header)
                        df_corrected[header] = pd.to_datetime(
                            df.loc[max(indeces) + 1 :, col], errors="coerce"
                        )
                    elif type == "numeric":
                        headers = df.loc[indeces, col].dropna()
                        headers = headers.apply(
                            lambda x: (
                                re.search(pattern, x).group(1)
                                if re.search(pattern, x) != None
                                else x
                            )
                        )
                        if len(headers) == 0:
                            header = "missing"
                        else:
                            header = "|".join(
                                str(item)
                                for item in headers.dropna().values
                                if len(item) < max_header_length
                            )
                            if header in prev_name:
                                header = header + "_2"
                        df_corrected[header] = pd.to_numeric(
                            df.loc[max(indeces) + 1 :, col], errors="coerce"
                        )
                    else:
                        headers = df.loc[indeces, col].dropna()
                        headers = headers.apply(
                            lambda x: (
                                re.search(pattern, x).group(1)
                                if re.search(pattern, x) != None
                                else x
                            )
                        )
                        if len(headers) == 0:
                            header = "missing"
                        else:
                            header = "|".join(
                                str(item)
                                for item in headers.dropna().values
                                if len(item) < max_header_length
                            )
                        df_corrected[header] = df.loc[max(indeces) + 1 :, col]

                    prev_name.append(header)

                    if sheet_name == "XML_file":
                        prev_names = [
                            x for x in df_corrected.columns if x not in datetime_name
                        ]
                        new_names = [
                            "Hacienda{}".format(i)
                            for i in range(1, len(prev_names) + 1)
                        ]
                        df_corrected = df_corrected.rename(
                            columns=dict(zip(prev_names, new_names))
                        )

                if df_corrected.shape[1] > 2:
                    df_corrected = drop_sum_column(df_corrected)

                for col in df_corrected.columns:
                    df_corrected = df_corrected[~df_corrected[col].isna()]

                if "Time Stamp" not in df_corrected.columns:
                    cols_corr = [
                        col.split("|")[1]
                        for col in df_corrected.columns
                        if col.__contains__("Time Stamp")
                    ][0]
                    cols_err = [
                        col.split("|")[0]
                        for col in df_corrected.columns
                        if col.__contains__("Time Stamp")
                    ][0]
                    other = df_corrected.columns[
                        df_corrected.columns != cols_err + "|" + cols_corr
                    ].values[0]
                    df_corrected = df_corrected.rename(
                        columns={
                            cols_err + "|" + cols_corr: cols_corr,
                            other: cols_err + "|" + other,
                        }
                    )

                # 6. Check the column names, fill blanks with previous column name
                pattern_2 = re.compile(r"^Unnamed: \d+$")
                df_corrected.columns = [
                    (
                        col
                        if not pattern_2.search(str(col))
                        else df_corrected.columns[i - 1]
                    )
                    for i, col in enumerate(df_corrected.columns)
                ]

                df_corrected.columns = [
                    col if not pd.isna(col) else df_corrected.columns[i - 1]
                    for i, col in enumerate(df_corrected.columns)
                ]

                # 7. Add the following columns: Excel Filename and Excel Sheet name
                df_corrected["Excel Filename"] = filename
                df_corrected["Excel Sheet"] = sheet_name

                # Second part: Making them all "Tabular"
                always_values = [
                    "Time Stamp"
                ]  # Date kinda values... Could have been identified in a different way.
                string_cols = list(
                    df_corrected.select_dtypes(include="object")
                )  # All the values that are a string.
                numeric_variables = df_corrected.columns[
                    ~df_corrected.columns.isin(always_values + string_cols)
                ]
                df_corrected = pd.melt(
                    df_corrected,
                    id_vars=always_values + string_cols,
                    value_vars=numeric_variables,
                    var_name="variables",
                    value_name="nominal",
                )

                # 8. All column names in lower caps.
                df_corrected.columns = df_corrected.columns.str.lower()

                all_dataframes.append(df_corrected)

    return all_dataframes


def transform_table(all_dataframes, modified_date_files):
    """
    Transform the all_dataframes list containing DataFrames into the final table.

    Args:
        all_dataframes (list):  List containing all the Parsed Pandas DataFrames
        modified_date_files (pd.DataFrame): pandas DataFrame containing a column with modified datetime.

    Return:
        combined_df (pd.DataFrame): Final Table with southern_2 Data
    """
    # Concatenate all the dataframes into one
    combined_df = pd.concat(all_dataframes, ignore_index=True)

    combined_df["variables"] = combined_df["variables"].str.split("|", expand=True)[0]
    combined_df["variables"] = combined_df["variables"].str.replace(" ", "")
    combined_df["variables"] = combined_df["variables"].str.replace("(MW)", "")

    totals_list = []
    reg_2 = "(?i)total\w*|all"

    for x in combined_df["variables"].unique():
        if re.search(reg_2, x) != None:
            totals_list.append(re.search(reg_2, x).string)

    combined_df = combined_df[~combined_df["variables"].isin(totals_list)]

    to_symbol = dict(zip(hacienda, variable_symb))
    to_number = dict(zip(variable_symb, hacienda))

    combined_df["Hacienda_symbol"] = combined_df["variables"].apply(
        lambda x: to_symbol[x] if x in hacienda else x
    )
    combined_df = combined_df.merge(
        modified_date_files, how="left", on="excel filename"
    )
    last_df = (
        combined_df.groupby(by=["time stamp"])
        .agg({"modified_date": "max"})
        .reset_index()
    )
    combined_df = combined_df.merge(last_df, how="left", on=["time stamp"])
    combined_df = combined_df[
        combined_df["modified_date_x"] == combined_df["modified_date_y"]
    ]
    combined_df = combined_df.drop(columns=["modified_date_x", "modified_date_y"])
    combined_df = (
        combined_df.groupby(by=["time stamp", "Hacienda_symbol"])
        .agg(
            {
                "excel filename": lambda x: "|".join(x),
                "excel sheet": lambda x: "|".join(x),
                "nominal": "mean",
            }
        )
        .reset_index()
    )
    combined_df["Hacienda_num"] = combined_df["Hacienda_symbol"].map(to_number)
    combined_df = combined_df[
        [
            "time stamp",
            "Hacienda_symbol",
            "Hacienda_num",
            "nominal",
            "excel filename",
            "excel sheet",
        ]
    ].rename(columns={"time stamp": "datetime"})
    combined_df = combined_df[
        combined_df["datetime"]
        .dt.to_period("min")
        .astype(str)
        .str.split(":", expand=True)[1]
        == "00"
    ]

    return combined_df


def load_table(combined_df):
    """
    Save tables in the outputs folder.

    Args:
        combined_df (pd.DataFrame): Final Table with southern_2 Data
    """

    combined_df.to_csv(os.path.join(project_dir, "outputs", "southern_2_table.csv"))
    combined_df.to_parquet(
        os.path.join(project_dir, "outputs", "southern_2_table.parquet")
    )


def parse_and_load_data():
    modified_date_files = get_last_modified_filelist(
        directory=directory, black_list=black_list
    )
    all_dataframes = parse_all_files(directory=directory, black_list=black_list)
    combined_df = transform_table(
        all_dataframes=all_dataframes, modified_date_files=modified_date_files
    )
    load_table(combined_df=combined_df)
    print("southern_2 Data Loaded Succesfully")


if __name__ == "__main__":
    parse_and_load_data()
