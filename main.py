import csv
import os
import psycopg2
from datetime import datetime


# File path and name.
filePath = 'd:\\neostudy\\'
fileName = 'dm_f101_round_f.csv'

# Database connection variable.
connect = None

# Check if the file path exists.
if os.path.exists(filePath):

    try:

        # Connect to database.
        connect = psycopg2.connect(host='localhost', database='neostudy',
                                   user='postgres', password='123')

    except psycopg2.DatabaseError as e:

        # Confirm unsuccessful connection and stop program execution.
        print("Database connection unsuccessful.")
        quit()

    # Cursor to execute query.
    cursor = connect.cursor()

    # SQL to select data from the person table.
    sqlSelect = \
        "SELECT from_date, to_date, chapter, ledger_account, characteristic, balance_in_rub, r_balance_in_rub, balance_in_val, r_balance_in_val, balance_in_total, r_balance_in_total, turn_deb_rub, r_turn_deb_rub, turn_deb_val, r_turn_deb_val, turn_deb_total, r_turn_deb_total, turn_cre_rub, r_turn_cre_rub, turn_cre_val, r_turn_cre_val, turn_cre_total, r_turn_cre_total, balance_out_rub, r_balance_out_rub, balance_out_val, r_balance_out_val, balance_out_total, r_balance_out_total \
         FROM DM.DM_F101_ROUND_F"

    try:

        # Execute query.
        cursor.execute(sqlSelect)

        # Fetch the data returned.
        results = cursor.fetchall()

        # Extract the table headers.
        headers = [i[0] for i in cursor.description]

        # Open CSV file for writing.
        csvFile = csv.writer(open(filePath + fileName, 'w', newline=''),
                             delimiter=',', lineterminator='\r\n') # quoting=csv.QUOTE_ALL, escapechar='\\'

        # Add the headers and data to the CSV file.
        csvFile.writerow(headers)
        csvFile.writerows(results)

        # Message stating export successful.
        print("Data export successful.")
        log_status = "Success"

    except psycopg2.DatabaseError as e:

        # Message stating export unsuccessful.
        print("Data export unsuccessful.")
        log_status = "Fail"
        quit()

    finally:
        log_query = "INSERT INTO dm.logs_csv (operation, log_time, status) VALUES (%s, %s, %s)"
        cursor.execute(log_query, ("Export data from dm.dm_f101_round_f into CSV ", datetime.now(), log_status))
        connect.commit()
        # Close database connection.
        connect.close()

else:

    # Message stating file path does not exist.
    print("File path does not exist.")