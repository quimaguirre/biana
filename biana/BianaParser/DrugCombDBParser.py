from bianaParser import *
import csv

class DrugCombDBParser(BianaParser):
    """
    MyData Parser Class 

    Parses data from the FDA Orange Book

    """

    name = "DrugCombDB"
    description = "This file implements a program that fills up tables in BIANA database from data in the FDA Orange Book"
    external_entity_definition = "An external entity represents a drug or a protein"
    external_entity_relations = "An external relation represents a drug combination or an interaction between drugs or drug-target"

    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "FDA Orange Book parser",  
                             default_script_name = "DrugCombDBParser.py",
                             default_script_description = DrugCombDBParser.description,     
                             additional_compulsory_arguments = [])
        self.default_eE_attribute = "name"

    def parse_database(self):
        """
        Method that implements the specific operations of a MyData formatted file
        """

        ###########################
        #### CHECK INPUT FILES ####
        ###########################

        # Check that all the files exist
        if os.path.isdir(self.input_file):
            self.input_path = self.input_file
        else:
            raise ValueError("You must specify a path instead of a file")

        required_files = ['drug_chemical_info.csv', 'SynDrugComb_external_ASDCDsynergism.txt', 'SynDrugComb_textmining_2drugs.txt', 'SynDrugComb_fda_all.txt']

        for current_file in required_files:
            if os.path.exists( os.path.join(self.input_path, current_file) ) is False:
                raise ValueError('File {} is missing in {}'.format(current_file, self.input_path))


        ########################
        #### PARSE DATABASE ####
        ########################

        parser = DrugCombDB(self.input_path)
        parser.parse_drug_chemical_info()
        parser.parse_asdcd_combinations(drugname_to_pubchem=parser.drugname_to_pubchem)
        #parser.parse_fda_combinations(drugname_to_pubchem=parser.drugname_to_pubchem)
        parser.parse_textmining_combinations(drugname_to_pubchem=parser.drugname_to_pubchem)
        sys.exit(10)


        #########################################
        #### INSERT PARSED DATABASE IN BIANA ####
        #########################################

        self.external_entity_ids_dict = {}

        #--------------#
        # INSERT DRUGS #
        #--------------#

        print("\n.....INSERTING THE DRUGS IN THE DATABASE.....\n")

        for pubchem in parser.pubchems:

            # Create an external entity corresponding to the drug in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(pubchem):

                self.create_drug_external_entity(parser, pubchem)
                #print(pubchem)

        print('{} drugs inserted'.format(len(parser.pubchems)))

        #--------------------------#
        # INSERT DRUG COMBINATIONS #
        #--------------------------#

        return


    def create_drug_external_entity(self, parser, pubchem):
        """
        Create an external entity of a drug
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "drug" )

        # Annotate it as PubChemCompound
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "PubChemCompound", value=pubchem, type="cross-reference") )

        if pubchem in parser.pubchem_to_drugname:
            for drugname in parser.pubchem_to_drugname[pubchem]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Name", value=drugname, type="cross-reference") )

        if pubchem in parser.pubchem_to_drugnameofficial:
            for drugnameofficial in parser.pubchem_to_drugnameofficial[pubchem]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Name", value=drugnameofficial, type="cross-reference") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[pubchem] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return




class DrugCombDB(object):

    def __init__(self, input_path):

        self.input_path = input_path
        self.drug_chemical_info_file = os.path.join(self.input_path, 'drug_chemical_info.csv')
        self.asdcd_file = os.path.join(self.input_path, 'SynDrugComb_external_ASDCDsynergism.txt')
        self.textmining_file = os.path.join(self.input_path, 'SynDrugComb_textmining_2drugs.txt')
        self.fda_file = os.path.join(self.input_path, 'SynDrugComb_fda_all.txt')

        self.pubchems = set()
        self.pubchem_to_drugname = {}
        self.pubchem_to_drugnameofficial = {}
        self.drugname_to_pubchem = {}

        self.combinations = set()
        self.combination_to_dcdb = {}
        self.pubchem_to_dcdb = {}
        self.combination_to_pubchem = {}

        return


    def parse_drug_chemical_info(self):
        """
        Parsing of drug chemical info file
        """

        print("\n.....PARSING DRUG CHEMICAL INFO FILE.....\n")

        num_line = 0

        with open(self.drug_chemical_info_file, 'r') as drug_chemical_info_fd:

            csvreader = csv.reader(drug_chemical_info_fd, delimiter=',')

            for fields in csvreader:

                num_line += 1

                # Obtain a dictionary: "field_name" : "position"
                if num_line == 1:
                    #drugName,cIds,drugNameOfficial,molecularWeight,smilesString
                    fields_dict = self.obtain_header_fields( ','.join(fields), separator=',')
                    continue

                # Get useful fields
                drugname = fields[ fields_dict['drugName'] ].lower()
                drugnameofficial = fields[ fields_dict['drugNameOfficial'] ].lower()
                pubchem_cid = fields[ fields_dict['cIds'] ].upper() # One per drug (e.g. CIDs00060750)

                # Check if application numbers, ingredients and trade names are available
                if drugname == '' or drugname == '#n/a':
                    print('Drug name unknown for PubChem {}'.format(pubchem_cid))
                    sys.exit(10)
                if pubchem_cid == '' or pubchem_cid == '#N/A':
                    print('Pubchem unknown for drug {}'.format(drugname))
                    sys.exit(10)

                # Add pubchem
                if pubchem_cid.startswith('CIDS'):
                    pubchem_cid = int(pubchem_cid.split('CIDS')[1])
                else:
                    print('Unknown format for PubChem CID format for drug {}: {}'.format(drugname, pubchem_cid))
                    continue
                    #sys.exit(10)
                self.pubchems.add(pubchem_cid)
                self.pubchem_to_drugname.setdefault(pubchem_cid, set()).add(drugname)
                self.drugname_to_pubchem.setdefault(drugname, set()).add(pubchem_cid)

                if drugnameofficial != '' and drugnameofficial != '#n/a':
                    self.pubchem_to_drugnameofficial.setdefault(pubchem_cid, set()).add(drugnameofficial)
                    self.drugname_to_pubchem.setdefault(drugnameofficial, set()).add(pubchem_cid)

        #print(self.pubchem_to_drugnameofficial)

        return


    def parse_asdcd_combinations(self, drugname_to_pubchem):
        """
        Parsing of ASDCD combinations file
        """

        print("\n.....PARSING ASDCD DRUG COMBINATIONS FILE.....\n")

        drugs_found = set()
        drugs_not_found = set()

        with open(self.asdcd_file, 'r') as asdcd_fd:

            csvreader = csv.reader(asdcd_fd, delimiter='\t')

            for fields in csvreader:

                drugname_A = fields[0].lower()
                drugname_B = fields[1].lower()
                pubmed = fields[2] # If there is no pubmed, the column has a number "1"
                pubmed_link = fields[3] # If there is no pubmed link, the column has a number "1"
                article = fields[4] # If there is no article, the column has a number "1"

                if drugname_A in drugname_to_pubchem:
                    drugs_found.add(drugname_A)
                else:
                    drugs_not_found.add(drugname_A)
                    #print('Drug without pubchem: {}'.format(drugname_A))
                if drugname_B in drugname_to_pubchem:
                    drugs_found.add(drugname_B)
                else:
                    drugs_not_found.add(drugname_B)
                    #print('Drug without pubchem: {}'.format(drugname_B))

                #drug_combination = frozenset([drugname_A, drugname_B])

        print('Number of drugs with PubChem found: {}'.format(len(drugs_found)))
        print('Number of drugs without PubChem found: {}'.format(len(drugs_not_found)))

        return


    def parse_fda_combinations(self, drugname_to_pubchem):
        """
        Parsing of FDA combinations file.
        These are the same drug combinations as in DCDB.
        """

        print("\n.....PARSING FDA DRUG COMBINATIONS FILE.....\n")

        drugs_found = set()
        drugs_not_found = set()
        num_line = 0

        with open(self.fda_file, 'r') as fda_fd:

            csvreader = csv.reader(fda_fd, delimiter='\t')

            for fields in csvreader:

                num_line += 1

                # Obtain a dictionary: "field_name" : "position"
                if num_line == 1:
                    #ID	PubChem_ID	Single PubChem_ID	DC_ID 	COMPONENT	MACHENISM	DCC_ID
                    fields_dict = self.obtain_header_fields( '\t'.join(fields), separator='\t')
                    continue

                comb_id = fields[ fields_dict['ID'] ]
                comb_pubchem_id = fields[ fields_dict['PubChem_ID'] ].upper()
                pubchem_ids = fields[ fields_dict['Single PubChem_ID'] ].upper().split('/')
                comb_dcdb_id = fields[ fields_dict['DC_ID'] ].upper()
                drug_names = fields[ fields_dict['COMPONENT'] ].lower()
                dcdb_ids = fields[ fields_dict['DCC_ID'] ].upper().split('/')

                combination = frozenset([])
                if 'NULL' in pubchem_ids:
                    print('One of the drugs of the combination {} without pubchem ID: {}'.format(comb_id, pubchem_ids))
                else:
                    for pubchem_id in pubchem_ids:
                        combination.append(pubchem_id)


        return


    def parse_textmining_combinations(self, drugname_to_pubchem):
        """
        Parsing of text mining combinations file
        """

        print("\n.....PARSING TEXT MINING DRUG COMBINATIONS FILE.....\n")

        drugs_found = set()
        drugs_not_found = set()
        num_line = 0

        with open(self.textmining_file, 'r') as textmining_fd:

            csvreader = csv.reader(textmining_fd, delimiter='\t')

            for fields in csvreader:

                num_line += 1

                # Obtain a dictionary: "field_name" : "position"
                if num_line == 1:
                    #drug1 	drug2	target	Source_Pubchem_
                    #curcumin	genistein	rhodesain of Trypanosoma brucei rhodesiense	29897253
                    fields_dict = self.obtain_header_fields( '\t'.join(fields), separator='\t')
                    continue

                drugname_A = fields[ fields_dict['drug1'] ].lower()
                drugname_B = fields[ fields_dict['drug2'] ].lower()
                comb_pubchem_id = fields[ fields_dict['Source_Pubchem_'] ].lower()

                if drugname_A == '' or drugname_A == 'null':
                    print('Drug name not available!')
                    sys.exit(10)
                if drugname_B == '' or drugname_B == 'null':
                    print('Drug name not available!')
                    sys.exit(10)
                if comb_pubchem_id == '' or comb_pubchem_id == 'null':
                    print('Pubchem not available for combination: {} - {}'.format(drugname_A, drugname_B))
                    sys.exit(10)

                if drugname_A in drugname_to_pubchem:
                    drugs_found.add(drugname_A)
                else:
                    drugs_not_found.add(drugname_A)
                    #print('Drug without pubchem: {}'.format(drugname_A))
                if drugname_B in drugname_to_pubchem:
                    drugs_found.add(drugname_B)
                else:
                    drugs_not_found.add(drugname_B)
                    #print('Drug without pubchem: {}'.format(drugname_B))

        print('Number of drugs with PubChem found: {}'.format(len(drugs_found)))
        print('Number of drugs without PubChem found: {}'.format(len(drugs_not_found)))

        return


    def obtain_header_fields(self, first_line, separator='\t'):
        """ 
        Obtain a dictionary: "field_name" => "position" 
        """
        fields_dict = {}

        header_fields = first_line.strip().split(separator)
        for x in xrange(0, len(header_fields)):
            fields_dict[header_fields[x]] = x

        return fields_dict


if __name__ == "__main__":
    main()

