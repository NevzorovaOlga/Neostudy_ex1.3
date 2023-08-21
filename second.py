import csv
import os
import psycopg2
from datetime import datetime

# File path.
filePath = 'd:\\neostudy\\dm_f101_round_f.csv'

# Database connection variable.
connect = None

# Check if the CSV file exists.
if os.path.isfile(filePath):

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

    # Assign CSV file to reader object.
    reader = csv.DictReader(open(filePath))

    # Record count.
    recordCount = 0

    # Insert person information into the database.
    for row in reader:

        # SQL to insert person information.
        sqlInsert = \
            "INSERT INTO DM.DM_F101_ROUND_F_V2 (from_date, to_date, chapter, ledger_account, characteristic, balance_in_rub, r_balance_in_rub, balance_in_val, r_balance_in_val, balance_in_total, r_balance_in_total, turn_deb_rub, r_turn_deb_rub, turn_deb_val, r_turn_deb_val, turn_deb_total, r_turn_deb_total, turn_cre_rub, r_turn_cre_rub, turn_cre_val, r_turn_cre_val, turn_cre_total, r_turn_cre_total, balance_out_rub, r_balance_out_rub, balance_out_val, r_balance_out_val, balance_out_total, r_balance_out_total)  \
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        try:

            # Execute query and commit changes.
            cursor.execute(sqlInsert, (row['from_date'],
                                       row['to_date'],
                                       row['chapter'],
                                       row['ledger_account'],
                                       row['characteristic'],
                                       row['balance_in_rub'],
                                       row['r_balance_in_rub'],
                                       row['balance_in_val'],
                                       row['r_balance_in_val'],
                                       row['balance_in_total'],
                                       row['r_balance_in_total'],
                                       row['turn_deb_rub'],
                                       row['r_turn_deb_rub'],
                                       row['turn_deb_val'],
                                       row['r_turn_deb_val'],
                                       row['turn_deb_total'],
                                       row['r_turn_deb_total'],
                                       row['turn_cre_rub'],
                                       row['r_turn_cre_rub'],
                                       row['turn_cre_val'],
                                       row['r_turn_cre_val'],
                                       row['turn_cre_total'],
                                       row['r_turn_cre_total'],
                                       row['balance_out_rub'],
                                       row['r_balance_out_rub'],
                                       row['balance_out_val'],
                                       row['r_balance_out_val'],
                                       row['balance_out_total'],
                                       row['r_balance_out_total']))
            connect.commit()

            # Increment the record count.
            recordCount += 1
            log_status = "Success"

        except psycopg2.DatabaseError as e:

            # Confirm error adding person information and stop program execution.
            print("Error adding information.")
            log_status = "Fail"
            quit()



    # Provide feedback on the number of records added.
    if recordCount == 0:

        print("No new records added.")

    elif recordCount == 1:

        print(str(recordCount) + " record added.")

    else:

        print(str(recordCount) + " records added.")

else:

    # Message stating CSV file could not be located.
    print("Could not locate the CSV file.")

log_query = "INSERT INTO dm.logs_csv (operation, log_time, status) VALUES (%s, %s, %s)"
cursor.execute(log_query, ("Import CSV into dm.dm_f101_round_f_v2", datetime.now(), log_status))
connect.commit()

# Close database connection.
connect.close()