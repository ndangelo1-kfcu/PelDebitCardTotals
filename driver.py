import pyodbc
from pyodbc import drivers
from datetime import datetime, timedelta
import os
import logging
import shutil
import re

# Ensure the logs directory exists
log_directory = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_directory, exist_ok=True)

# Configure logging to log to both a file and the console
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all log messages

# Create file handler
file_handler = logging.FileHandler(os.path.join(log_directory, "process_log.log"))
file_handler.setLevel(logging.DEBUG)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create formatter and add it to the handlers
formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Define the directory and search directory for the file
directory = r"C:\kdev\PY_Nate\PELDEBITCARDTOTALS\EFT_SOURCE_FILES"
archive_directory = r"C:\kdev\PY_Nate\PELDEBITCARDTOTALS\Archive"
checkpoint_file = "checkpoint.txt"

# Define the column indices (adjust these as needed)
card_num_col_index = (21, 38)
acct_num_col_index = (42, 52)
name_col_index = (87, 140)
address_col_index = (199, 250)
city_col_index = (259, 277)
zipcode_col_index = (277, 291)
ref_num_col_index = (372, 390)
dba_col_index = (550, 577)


# Function to parse a fixed-width line
def parse_fixed_width_line(line):
    ref_num = line[ref_num_col_index[0] : ref_num_col_index[1]].strip()
    acct_num = line[acct_num_col_index[0] : acct_num_col_index[1]].strip()
    card_num = line[card_num_col_index[0] : card_num_col_index[1]].strip()
    name = line[name_col_index[0] : name_col_index[1]].strip()
    address = line[address_col_index[0] : address_col_index[1]].strip()
    city = line[city_col_index[0] : city_col_index[1]].strip()
    zipcode = line[zipcode_col_index[0] : zipcode_col_index[1]].strip()
    dba = line[dba_col_index[0] : dba_col_index[1]].strip()
    return ref_num, acct_num, card_num, name, address, city, zipcode, dba


def create_connection(database_name):
    # Set up SQL connection
    if "ODBC Driver 17 for SQL Server" in drivers():
        odbcDriver = "ODBC Driver 17 for SQL Server"
    elif "ODBC Driver 13.1 for SQL Server" in drivers():
        odbcDriver = "ODBC Driver 13.1 for SQL Server"
    elif "ODBC Driver 13 for SQL Server" in drivers():
        odbcDriver = "ODBC Driver 13 for SQL Server"
    else:
        odbcDriver = ""
        # raise FunctionError("verifydriver", "Missing database driver")

    connection_string = (
        f"DRIVER={odbcDriver};"
        "SERVER=VSARCU02;"
        f"DATABASE={database_name};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(connection_string)


def change_database(cursor, new_database_name):
    cursor.execute(f"USE {new_database_name}")


# Function to read the checkpoint file
def read_checkpoint():
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            checkpoints = {}
            for line in f:
                filename, line_number = line.strip().split(",")
                checkpoints[filename] = int(line_number)
            return checkpoints
    return {}


# Function to update the checkpoint file
def update_checkpoint(filename, line_number):
    checkpoints = read_checkpoint()
    checkpoints[filename] = line_number
    with open(checkpoint_file, "w") as f:
        for file, line_num in checkpoints.items():
            f.write(f"{file},{line_num}\n")


def process_file_list(filename):
    file_path = os.path.join(directory, filename)
    if os.path.isfile(file_path):  # Adjust the file format as needed
        try:
            # Create a new connection for each file
            conn = create_connection("ARCUSYM000")
            cursor = conn.cursor()

            # Log the file being processed
            logging.info(f"Processing file: {filename}")

            # Read the file and process the first line for the process date
            with open(file_path, mode="r") as file:
                # Skip the first 14 lines
                for _ in range(16):
                    file.readline()

                # Read the 16th line for the process date
                process_date_line = file.readline().rstrip("\n\r")
                process_date_str = process_date_line[32:39].strip()
                print(
                    f"Extracted process date string: '{process_date_str}'"
                )  # Debugging step
                try:
                    process_date = datetime.strptime(
                        process_date_str, "%m%d%y"
                    ).strftime("%Y%m%d")
                    process_date_int = int(process_date)
                except ValueError as ve:
                    logging.error(
                        f"Error parsing process date from string '{process_date_str}': {ve}"
                    )
                    return

                # Regular expression to match lines that begin with a 6-digit number followed by 4 spaces and a two-digit number
                pattern1 = re.compile(r"^\d{6}\s{4}\d{2}")
                pattern2 = re.compile(
                    r"^\s*(\bPO BOX\b|\d{1,5}\s[A-Z][A-Z\s]+)\s*.*\s*$"
                )
                pattern3 = re.compile(r"^\s*.*?\b[A-Z]{2}\d{5}(-\d{4})?\b.*\s*$")
                # Read the rest of the file and process matching lines
                line_number = 17  # Start after the initial 16 lines
                while True:
                    line1 = file.readline().rstrip("\n\r")
                    line_number += 1
                    if line1.startswith("Record Count:"):
                        break  # End of file or end of records

                    if pattern1.match(line1):
                        line2 = file.readline().rstrip("\n\r")
                        line_number += 1
                        while not pattern2.match(line2):
                            line2 = file.readline().rstrip("\n\r")
                            line_number += 1

                        if line2.endswith("  "):
                            line2 = line2[:-2]

                        line3 = file.readline().rstrip("\n\r")
                        line_number += 1
                        while not pattern3.match(line3):
                            line3 = file.readline().rstrip("\n\r")
                            line_number += 1

                        combined_line = line1 + line2 + line3

                        (
                            ref_num,
                            acct_num,
                            card_num,
                            name,
                            address,
                            city,
                            zipcode,
                            dba,
                        ) = parse_fixed_width_line(combined_line)

                        # Skip if AccountNumber, ReferenceId, or CardNumber are null
                        if not acct_num or not ref_num or not card_num:
                            logging.warning(
                                f"Skipping line {line_number} due to missing AccountNumber, ReferenceId, or CardNumber."
                            )
                            continue

                        # Change the database to ARCUSYM000 to check if the account is new
                        change_database(cursor, "ARCUSYM000")

                        # Execute stored procedure with OUTPUT parameter
                        result = cursor.execute(
                            """
                            DECLARE @Result INT;
                            EXEC usp_IsNewAccount ?, @Result OUTPUT;
                            SELECT @Result;
                        """,
                            acct_num,
                        ).fetchone()

                        if result and result[0] == 1:
                            new_acct = "T"
                        else:
                            new_acct = "F"

                        change_database(cursor, "kRAP")

                        # Call the stored procedure to insert the values into the CardTotals table
                        cursor.execute(
                            """
                            EXEC debit.usp_UpsertCardTotals 
                            @ProcessDate = ?, 
                            @AccountNumber = ?, 
                            @ReferenceId = ?, 
                            @NewAcct = ?, 
                            @CardNumber = ?, 
                            @Name = ?, 
                            @Address = ?, 
                            @City = ?, 
                            @ZIPCODE = ?, 
                            @DBA = ?
                        """,
                            process_date_int,
                            acct_num,
                            ref_num,
                            new_acct,
                            card_num,
                            name,
                            address,
                            city,
                            zipcode,
                            dba,
                        )

                        # Commit the transaction after each line
                        conn.commit()

            cursor.close()
            conn.close()
            logging.info(f"Processed file: {filename}")

            # Move the processed file to the Archive directory
            shutil.move(file_path, os.path.join(archive_directory, filename))
            logging.info(f"Moved file to archive: {filename}")

            # Remove the checkpoint entry for the processed file
            update_checkpoint(filename, 0)
        except Exception as e:
            logging.error(f"Error processing file {filename}: {e}")
            print(f"Error processing file {filename}: {e}")


def process_file(filename):
    file_path = os.path.join(directory, filename)
    if os.path.isfile(file_path):  # Adjust the file format as needed
        try:
            # Create a new connection for each file
            conn = create_connection("ARCUSYM000")
            cursor = conn.cursor()

            # Log the file being processed
            logging.info(f"Processing file: {filename}")

            # Read the file and process the first line for the process date
            with open(file_path, mode="r") as file:
                first_row = file.readline().strip()
                process_date_str = first_row[32:39]
                process_date = datetime.strptime(process_date_str, "%m%d%y").strftime(
                    "%Y%m%d"
                )
                process_date_int = int(process_date)

                # Determine the starting line number
                start_line = checkpoints.get(filename, 1)

                # Read the rest of the file and count matches
                for current_line_number, line in enumerate(file, start=2):
                    if current_line_number < start_line:
                        continue

                    ref_num, acct_num, card_num, name, address, city, zipcode, dba = (
                        parse_fixed_width_line(line)
                    )

                    # Skip if AccountNumber, ReferenceId, or CardNumber are null
                    if not acct_num or not ref_num or not card_num:
                        logging.warning(
                            f"Skipping line {current_line_number} in file {filename} due to missing AccountNumber, ReferenceId, or CardNumber."
                        )
                        continue

                    # Change the database to ARCUSYM000 to check if the account is new
                    change_database(cursor, "ARCUSYM000")

                    # Execute stored procedure with OUTPUT parameter
                    result = cursor.execute(
                        """
                        DECLARE @Result INT;
                        EXEC usp_IsNewAccount ?, @Result OUTPUT;
                        SELECT @Result;
                    """,
                        acct_num,
                    ).fetchone()

                    if result and result[0] == 1:
                        new_acct = "T"
                    else:
                        new_acct = "F"

                    change_database(cursor, "kRAP")

                    # Call the stored procedure to insert the values into the CardTotals table
                    cursor.execute(
                        """
                        EXEC debit.usp_UpsertCardTotals 
                        @ProcessDate = ?, 
                        @AccountNumber = ?, 
                        @ReferenceId = ?, 
                        @NewAcct = ?, 
                        @CardNumber = ?, 
                        @Name = ?, 
                        @Address = ?, 
                        @City = ?, 
                        @ZIPCODE = ?, 
                        @DBA = ?
                    """,
                        process_date_int,
                        acct_num,
                        ref_num,
                        new_acct,
                        card_num,
                        name,
                        address,
                        city,
                        zipcode,
                        dba,
                    )

                    # Commit the transaction after each line
                    conn.commit()

                    # Update the checkpoint file after processing each line
                    update_checkpoint(filename, current_line_number)

            cursor.close()
            conn.close()
            logging.info(f"Processed file: {filename}")

            # Move the processed file to the Archive directory
            shutil.move(file_path, os.path.join(archive_directory, filename))
            logging.info(f"Moved file to archive: {filename}")

            # Remove the checkpoint entry for the processed file
            update_checkpoint(filename, 0)
        except Exception as e:
            logging.error(
                f"Error processing file {filename} at line {current_line_number}: {e}"
            )
            print(
                f"Error processing file {filename} at line {current_line_number}: {e}"
            )


# Get all files in the directory
checkpoints = read_checkpoint()
files_to_process = [
    filename
    for filename in os.listdir(directory)
    if os.path.isfile(os.path.join(directory, filename))
]

# Log if no files are found
if not files_to_process:
    logging.info("No files found to process.")

# Process each file sequentially
for filename in files_to_process:
    if "list" in filename.lower():
        process_file_list(filename)
    else:
        process_file(filename)

# Remove log files older than 90 days
log_file = os.path.join(log_directory, "process_log.log")
if os.path.exists(log_file):
    creation_time = datetime.fromtimestamp(os.path.getctime(log_file))
    if datetime.now() - creation_time > timedelta(days=90):
        os.remove(log_file)
        logging.info(f"Removed log file: {log_file}")
