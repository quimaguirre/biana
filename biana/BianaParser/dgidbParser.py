from bianaParser import *
                    
class dgidbParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from DGIdb

    """                 
                                                                                         
    name = "dgidb"
    description = "This file implements a program that fills up tables in BIANA database from data in DGIdb"
    external_entity_definition = "An external entity represents a drug or a protein"
    external_entity_relations = "An external relation represents a drug combination or an interaction between drugs or drug-target"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "DGIdb parser",  
                             default_script_name = "dgidbParser.py",
                             default_script_description = dgidbParser.description,     
                             additional_compulsory_arguments = [])
        self.default_eE_attribute = "chembl"
                    
    def parse_database(self):
        """                                                                              
        Method that implements the specific operations of a MyData formatted file
        """

        #####################################
        #### DEFINE NEW BIANA CATEGORIES ####
        #####################################

        # Add DrugBankID as a valid external entity attribute since it is not recognized by BIANA
        self.biana_access.add_valid_external_entity_attribute_type( name = "DGIDB_interaction_source",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE identifier attribute")

        # Add DrugBankID as a valid external entity attribute since it is not recognized by BIANA
        self.biana_access.add_valid_external_entity_attribute_type( name = "DGIDB_interaction_type",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE identifier attribute")

        # Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
        self.biana_access.refresh_database_information()



        ###########################
        #### CHECK INPUT FILES ####
        ###########################

        # Check that all the files exist
        if not os.path.isfile(self.input_file):
            raise ValueError('You must specify a file!')



        ########################
        #### PARSE DATABASE ####
        ########################

        parser = DGIdb(self.input_file)
        parser.parse()



        #########################################
        #### INSERT PARSED DATABASE IN BIANA ####
        #########################################

        print("\n.....INSERTING THE DRUGS IN THE DATABASE.....\n")

        self.external_entity_ids_dict = {}

        for chembl_id in parser.chembls:

            # Create an external entity corresponding to the drug in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(chembl_id):

                self.create_drug_external_entity(parser, chembl_id)


        print("\n.....INSERTING THE GENES IN THE DATABASE.....\n")

        for entrez_id in parser.entrez_ids:

            # Create an external entity corresponding to the drug in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(entrez_id):

                self.create_protein_external_entity(parser, entrez_id)


        print("\n.....INSERTING THE INTERACTIONS IN THE DATABASE.....\n")

        for interaction in parser.interactions:


            # CREATE THE EXTERNAL ENTITY RELATION
            # Create an external entity relation corresponding to interaction between two drugs in the database
            new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "interaction" )

            (entrez_id, chembl_id) = interaction

            # Associate the participants of the interaction
            new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[entrez_id] )
            new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[chembl_id] )

            if interaction in parser.interaction_to_source:

                for source in parser.interaction_to_source[interaction]:

                    # Associate the interaction with the source
                    new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier= "DGIDB_interaction_source", value=source, type="unique" ) )

            if interaction in parser.interaction_to_type:

                for interaction_type in parser.interaction_to_type[interaction]:

                    # Associate the interaction with the type
                    new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier= "DGIDB_interaction_type", value=interaction_type, type="unique" ) )


            # Insert this external entity relation into database
            self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return


    def create_drug_external_entity(self, parser, chembl_id):
        """
        Create an external entity of a drug
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "drug" )

        # Annotate it as CHEMBL
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "CHEMBL", value=chembl_id, type="cross-reference") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[chembl_id] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_protein_external_entity(self, parser, entrez_id):
        """
        Create an external entity of a protein
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        # Annotate it as GeneID
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneID", value=entrez_id, type="cross-reference") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[entrez_id] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return



class DGIdb(object):

    def __init__(self, input_file):

        self.interactions_file = input_file

        self.entrez_ids = set()
        self.chembls = set()
        self.interactions = set()
        self.sources = set()
        self.types = set()
        self.interaction_to_source = {}
        self.interaction_to_type = {}

        return

    def parse(self):


        #################################
        #### PARSE INTERACTIONS FILE ####
        #################################

        import csv

        print("\n.....PARSING INTERACTIONS FILE.....\n")

        with open(self.interactions_file,'r') as interactions_file_fd:

            first_line = interactions_file_fd.readline()

            # Obtain a dictionary: "field_name" => "position"
            # gene_name gene_claim_name entrez_id   interaction_claim_source    interaction_types   drug_claim_name drug_claim_primary_name drug_name   drug_chembl_id
            fields_dict = self.obtain_header_fields(first_line, separator='\t')

            csvreader = csv.reader(interactions_file_fd, delimiter='\t')
            for fields in csvreader:

                entrez_id = fields[ fields_dict['entrez_id'] ]
                source = fields[ fields_dict['interaction_claim_source'] ].lower()
                interaction_types = fields[ fields_dict['interaction_types'] ].lower()
                chembl_id = fields[ fields_dict['drug_chembl_id'] ].upper()

                if entrez_id != '' and chembl_id != '':
                    interaction = (entrez_id, chembl_id)
                    self.interactions.add(interaction)
                    self.entrez_ids.add(entrez_id)
                    self.chembls.add(chembl_id)

                    if source != '':
                        self.sources.add(source)
                        self.interaction_to_source.setdefault(interaction, set())
                        self.interaction_to_source[interaction].add(source)
                    else:
                        print('No source in interaction {}\n'.format(interaction))
                        sys.exit(10)

                    if interaction_types != '':
                        interaction_types = interaction_types.split(',')
                        for interaction_type in interaction_types:
                            self.types.add(interaction_type)
                            self.interaction_to_type.setdefault(interaction, set())
                            self.interaction_to_type[interaction].add(interaction_type)

        return



    def obtain_header_fields(self, first_line, separator='\t'):
        """ 
        Obtain a dictionary: "field_name" => "position" 
        """
        fields_dict = {}

        header_fields = first_line.strip().split(separator)
        for x in xrange(0, len(header_fields)):
            fields_dict[header_fields[x].lower()] = x

        return fields_dict


if __name__ == "__main__":
    main()

