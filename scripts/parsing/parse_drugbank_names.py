import os, sys, re
import mysql.connector


#DATA_DIR = "/home/quim/PHD/Projects/BIANA/data"
DATA_DIR = "/Users/quim/Dropbox/UPF/PHD/Projects/BIANA/data"
DB_NAME = "test_2020" # test_BIANA_JUNE_2011
DB_USER = "quim"
DB_PASS = ""
DB_HOST = "localhost"
UNIX_SOCKET = None #"/home/emre/tmp/mysql.sock"

def main():

    cnx = mysql.connector.connect( user=DB_USER,
                               password=DB_PASS,
                               host=DB_HOST,
                               database=DB_NAME )
    cursor = cnx.cursor()

    output_file = os.path.join(DATA_DIR, 'drugbankID_to_names.txt')
    drugbank_to_names = get_data_from_DrugBank(cursor)
    cursor.close()
    cnx.close()

    with open(output_file, 'w') as out_fd:
        out_fd.write('DrugBankID\tDrug name\tType of name\n')
        for drugbank in drugbank_to_names:
            for (drug_name, type_name) in drugbank_to_names[drugbank]:
                out_fd.write('{}\t{}\t{}\n'.format(drugbank, drug_name, type_name))

    return

def get_data_from_DrugBank(cursor):
    """
    Get drug info from DrugBank
    """
    query1 = ("""SELECT D.value, N.value, N.type 
                 FROM externalEntity E, externalDatabase DB, externalEntityDrugBankID D, externalEntityName N 
                 WHERE E.externalDatabaseID = DB.externalDatabaseID AND E.externalEntityID = D.externalEntityID AND E.externalEntityID = N.externalEntityID AND E.type = "drug" AND DB.databaseName = "drugbank"
              """)

    drugs = set()

    cursor.execute(query1)

    drugbank_to_names = {}
    for row in cursor:
        drugbank_id, drug_name, type_name = row
        drugbank_to_names.setdefault(drugbank_id.upper(), set()).add((drug_name.lower(), type_name.lower()))

    return drugbank_to_names


if __name__ == "__main__":
    main()

