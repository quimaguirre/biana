from bianaParser import *
import csv

class FDAOrangeBookParser(BianaParser):
    """
    MyData Parser Class 

    Parses data from the FDA Orange Book

    """

    name = "FDAOrangeBook"
    description = "This file implements a program that fills up tables in BIANA database from data in the FDA Orange Book"
    external_entity_definition = "An external entity represents a drug or a protein"
    external_entity_relations = "An external relation represents a drug combination or an interaction between drugs or drug-target"

    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "FDA Orange Book parser",  
                             default_script_name = "FDAOrangeBookParser.py",
                             default_script_description = FDAOrangeBookParser.description,     
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

        required_files = ['products.txt', 'patent.txt', 'exclusivity.txt']

        for current_file in required_files:
            if os.path.exists( os.path.join(self.input_path, current_file) ) is False:
                raise ValueError('File {} is missing in {}'.format(current_file, self.input_path))


        ########################
        #### PARSE DATABASE ####
        ########################

        parser = FDAOrangeBook(self.input_path)
        parser.parse_products_file()


        #########################################
        #### INSERT PARSED DATABASE IN BIANA ####
        #########################################

        self.external_entity_ids_dict = {}

        #--------------#
        # INSERT DRUGS #
        #--------------#

        print("\n.....INSERTING THE DRUGS IN THE DATABASE.....\n")

        for appl_no in parser.appl_numbers:

            # Create an external entity corresponding to the drug in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(appl_no):

                new_external_entity = ExternalEntity( source_database = self.database, type = "drug" )

                # Annotate it as CHEMBL
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "CHEMBL", value=drug_chembl, type="unique") )

                if drug_chembl in parser.drug_to_atcs:
                    for atc in parser.drug_to_atcs[drug_chembl]:
                        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "ATC", value=atc, type="cross-reference") )

                if drug_chembl in parser.drug_to_smiles:
                    smiles = parser.drug_to_smiles[drug_chembl]
                    new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "SMILES", value=smiles, type="cross-reference") )

                if drug_chembl in parser.drug_to_type:
                    drug_type = parser.drug_to_type[drug_chembl]
                    new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "CHEMBL_drug_type", value=drug_type, type="unique") )

                # Insert this external entity into BIANA
                self.external_entity_ids_dict[drug_chembl] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )



                self.create_drug_external_entity(parser, drug_chembl)
                #print(drug_chembl)

        print('{} drugs inserted'.format(len(parser.drugs)))


        #----------------#
        # INSERT TARGETS #
        #----------------#

        print("\n.....INSERTING THE PROTEINS IN THE DATABASE.....\n")
        uniprots_inserted = set()

        for target_chembl in parser.targets:

            if target_chembl in parser.target_chembl_to_uniprots:

                for target_uniprot in parser.target_chembl_to_uniprots[target_chembl]:

                    # Create an external entity corresponding to the target in the database (if it is not already created)
                    if not self.external_entity_ids_dict.has_key(target_uniprot):

                        self.create_protein_external_entity(parser, target_uniprot)
                        uniprots_inserted.add(target_uniprot)
                        #print(target_uniprot)

        print('{} proteins inserted'.format(len(uniprots_inserted)))


        #-----------------#
        # INSERT DISEASES #
        #-----------------#

        print("\n.....INSERTING THE DISEASES IN THE DATABASE.....\n")

        for disease_MESH in parser.diseases:

            # Create an external entity corresponding to the disease in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(disease_MESH):

                self.create_disease_external_entity(parser, disease_MESH)
                #print(disease_MESH)

        print('{} diseases inserted'.format(len(parser.diseases)))


        #---------------------#
        # INSERT INTERACTIONS #
        #---------------------#

        print("\n.....INSERTING THE INTERACTIONS IN THE DATABASE.....\n")
        interactions_inserted = set()

        for interaction in parser.interactions:

            (target_chembl, drug_chembl) = interaction
            #print(target_chembl, drug_chembl)

            if target_chembl in parser.target_chembl_to_uniprots:

                for target_uniprot in parser.target_chembl_to_uniprots[target_chembl]:

                    uniprot_drug_interaction = (target_uniprot, drug_chembl)

                    # Create an external entity corresponding to the interaction in the database (if it is not already created)
                    if not self.external_entity_ids_dict.has_key(uniprot_drug_interaction):

                        # Create an external entity relation corresponding to interaction between the drug and the target
                        new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "interaction" )

                        # Associate the participants of the interaction
                        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[target_uniprot] )
                        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[drug_chembl] )

                        if interaction in parser.interaction_to_action:

                            action = parser.interaction_to_action[interaction]

                            # Associate the interaction with the action of the drug against the target
                            new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier= "CHEMBL_action", value=action, type="unique" ) )

                        if interaction in parser.interaction_to_mechanism:

                            mechanism = parser.interaction_to_mechanism[interaction]

                            # Associate the interaction with the action of the drug against the target
                            new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier= "CHEMBL_mechanism", value=mechanism, type="unique" ) )

                        if interaction in parser.interaction_to_activity_values:

                            for activity_values in parser.interaction_to_activity_values[interaction]:

                                [standard_type, standard_relation, standard_value, standard_units] = activity_values

                                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier= "CHEMBL_drug_target_activity", 
                                                                                                             value=standard_value, 
                                                                                                             type="unique",
                                                                                                             additional_fields = {"activity_type": standard_type, "relation":standard_relation, "unit":standard_units} ) )


                        # Insert this external entity relation into database
                        self.external_entity_ids_dict[uniprot_drug_interaction] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

                        interactions_inserted.add(uniprot_drug_interaction)

        print('{} interactions inserted'.format(len(interactions_inserted)))


        #--------------------#
        # INSERT INDICATIONS #
        #--------------------#

        print("\n.....INSERTING THE INDICATIONS IN THE DATABASE.....\n")

        for indication in parser.indications:

            # Create an external entity corresponding to the interaction in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(indication):

                (mesh_id, drug_chembl) = indication
                #print(mesh_id, drug_chembl)

                # Create an external entity relation corresponding to interaction between the drug and the target
                new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "drug_indication_association" )

                # Associate the participants of the interaction
                new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[mesh_id] )
                new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[drug_chembl] )

                # Insert this external entity relation into database
                self.external_entity_ids_dict[indication] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        print('{} indications inserted'.format(len(parser.indications)))

        return


    def create_drug_external_entity(self, parser, drug_chembl):
        """
        Create an external entity of a drug
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "drug" )

        # Annotate it as CHEMBL
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "CHEMBL", value=drug_chembl, type="unique") )

        if drug_chembl in parser.drug_to_atcs:
            for atc in parser.drug_to_atcs[drug_chembl]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "ATC", value=atc, type="cross-reference") )

        if drug_chembl in parser.drug_to_smiles:
            smiles = parser.drug_to_smiles[drug_chembl]
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "SMILES", value=smiles, type="cross-reference") )

        if drug_chembl in parser.drug_to_type:
            drug_type = parser.drug_to_type[drug_chembl]
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "CHEMBL_drug_type", value=drug_type, type="unique") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[drug_chembl] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_protein_external_entity(self, parser, target_uniprot):
        """
        Create an external entity of a protein
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        # Annotate it as uniprot accession
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotAccession", value=target_uniprot, type="cross-reference") )

        # Annotate it's taxonomy identifiers (there can be multiple ones!)
        if target_uniprot in parser.uniprot_to_taxids:
            for taxid in parser.uniprot_to_taxids[target_uniprot]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "TaxID", value=taxid, type="cross-reference") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[target_uniprot] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_disease_external_entity(self, parser, disease_MESH):
        """
        Create an external entity of a disease and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "disease" )

        # Annotate its disease MESH
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "MESH_ID", value=disease_MESH.upper(), type="cross-reference") )

        # Annotate its disease MESH
        if disease_MESH in parser.meshid_to_meshheading:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "MESH_name", value=parser.meshid_to_meshheading[disease_MESH].lower(), type="cross-reference") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[disease_MESH] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return



class FDAOrangeBook(object):

    def __init__(self, input_path):

        self.input_path = input_path
        self.products_file = os.path.join(self.input_path, 'products.txt')
        self.patent_file = os.path.join(self.input_path, 'patent.txt')
        self.exclusivity_file = os.path.join(self.input_path, 'exclusivity.txt')

        self.appl_numbers = set()
        self.ingredients = set()
        self.appl_no_to_ingredients = {}
        self.appl_no_to_trade_name = {}
        self.appl_no_to_product_no = {}
        self.appl_no_to_product_type = {}
        self.appl_no_to_approval_date = {}

        self.drugname_to_id = {}
        self.drugname_to_id_to_types = {}

        return


    def parse_products_file(self):
        """
        Parsing of the products file of FDA Orange Book
        """

        print("\n.....PARSING PRODUCTS FILE.....\n")

        num_line = 0

        with open(self.products_file, 'r') as products_fd:

            csvreader = csv.reader(products_fd, delimiter='~')

            for fields in csvreader:

                num_line += 1

                # Obtain a dictionary: "field_name" : "position"
                if num_line == 1:
                    # Ingredient~DF;Route~Trade_Name~Applicant~Strength~Appl_Type~Appl_No~Product_No~TE_Code~Approval_Date~RLD~RS~Type~Applicant_Full_Name
                    # BUDESONIDE~AEROSOL, FOAM;RECTAL~UCERIS~SALIX~2MG/ACTUATION~N~205613~001~~Oct 7, 2014~Yes~Yes~RX~SALIX PHARMACEUTICALS INC
                    fields_dict = self.obtain_header_fields( '~'.join(fields), separator='~')
                    continue

                # Get useful fields
                current_ingredients = fields[ fields_dict['Ingredient'] ].lower() # There can be one ingredient (e.g. BUDESONIDE) or multiple (e.g. SACUBITRIL; VALSARTAN)
                trade_name = fields[ fields_dict['Trade_Name'] ].lower() # UCERIS
                appl_type = fields[ fields_dict['Appl_Type'] ].upper() # The type of new drug application approval. New Drug Applications (NDA or innovator) are ”N”. Abbreviated New Drug Applications (ANDA or generic) are “A”.
                appl_no = fields[ fields_dict['Appl_No'] ] # The FDA assigned number to the application. Format is nnnnnn (e.g. 205613)
                product_no = fields[ fields_dict['Product_No'] ] # The FDA assigned number to identify the application products. Each strength is a separate product.  May repeat for multiple part products. If there are multiple dosages, there will be multiple product numbers because each dosage is part of a different product, but they will still have the same application number (e.g. SOFOSBUVIR; VELPATASVIR). Format is nnn (e.g. 001).
                approval_date = fields[ fields_dict['Approval_Date'] ] # The date the product was approved as stated in the FDA approval letter to the applicant.  The format is Mmm dd, yyyy.  Products approved prior to the January 1, 1982 contain the phrase: "Approved prior to Jan 1, 1982". (e.g. Oct 7, 2014). There can be multiple lines with different dates depending on the dosage (e.g. SOFOSBUVIR; VELPATASVIR)
                product_type = fields[ fields_dict['Type'] ].upper() # The group or category of approved drugs. Format is RX, OTC, DISCN.

                #print(current_ingredients, trade_name, appl_no, product_no)

                # Check if application numbers, ingredients and trade names are available
                if appl_no == '':
                    print('Trade name product {} without application number!'.format(trade_name))
                    sys.exit(10)
                if current_ingredients == '' or trade_name == '': 
                    print('Missing trade name / ingredients for application number {}'.format(appl_no))
                    sys.exit(10)

                # Add application number
                self.appl_numbers.add(appl_no)

                # Check if multiple ingredients
                if '; ' in current_ingredients:
                    current_ingredients = current_ingredients.split('; ')
                elif ';' in current_ingredients:
                    # Examples:
                    # triple sulfa (sulfabenzamide;sulfacetamide;sulfathiazole)
                    # trisulfapyrimidines (sulfadiazine;sulfamerazine;sulfamethazine)
                    # liotrix (t4;t3)
                    #print('Multiple ingredients separated differently: {}'.format(current_ingredients))
                    current_ingredients = current_ingredients.split('; ')
                else:
                    current_ingredients = current_ingredients.split('; ')

                # Add ingredients
                for current_ingredient in current_ingredients:
                    self.appl_no_to_ingredients.setdefault(appl_no, set()).add(current_ingredient)
                    self.ingredients.add(current_ingredient)

                # Add trade name
                self.appl_no_to_trade_name[appl_no] = trade_name

        #print(self.appl_numbers)
        #print(self.ingredients)

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

