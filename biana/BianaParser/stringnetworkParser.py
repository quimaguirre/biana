from bianaParser import *
                    
class stringnetworkParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from the STRING network

    """                 
                                                                                         
    name = "stringnetwork"
    description = "This file implements a program that fills up tables in BIANA database from data in the STRING network"
    external_entity_definition = "An external entity represents a protein"
    external_entity_relations = "An external relation represents a interaction"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "STRING network",  
                             default_script_name = "stringnetworkParser.py",
                             default_script_description = stringnetworkParser.description,     
                             additional_compulsory_arguments = [])
        self.default_eE_attribute = "geneID"
                    
    def parse_database(self):
        """                                                                              
        Method that implements the specific operations of a MyData formatted file
        """

        ###########################
        #### CHECK INPUT FILES ####
        ###########################

        # Check that the input file exists
        if not os.path.isfile(self.input_file):
            raise ValueError("The input file is missing. Please, download the file \'IID_Sokolina_MSB_2017.txt\'")


        ########################
        #### PARSE DATABASE ####
        ########################

        species_to_taxID = {
            'human' : 9606,
        }
        species = 'human'

        # Parse the human interactome
        print("\n.....PARSING THE STRING HUMAN NETWORK.....\n")
        parser = STRINGnetwork(self.input_file)
        parser.parse()


        #########################################
        #### INSERT PARSED DATABASE IN BIANA ####
        #########################################

        # Defining the dict in which we will store the created external entities
        self.external_entity_ids_dict = {}


        print("\n.....INSERTING THE INTERACTORS IN THE DATABASE.....\n")

        for interactor in parser.interactors:
            if not self.external_entity_ids_dict.has_key(interactor):
                self.create_protein_external_entity(parser, interactor, species_to_taxID[species]) 

        print('{} interactors'.format(len(parser.interactors)))


        print("\n.....INSERTING THE INTERACTIONS IN THE DATABASE.....\n")

        for interaction in parser.interactions:
            self.create_functional_association_entity(parser, interaction)

        print('{} interactions'.format(len(parser.interactions)))

        print("\nPARSING OF STRING NETWORK FINISHED. THANK YOU FOR YOUR PATIENCE\n")

        return



    def create_protein_external_entity(self, parser, interactor, taxID):
        """
        Create an external entity of a protein and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        # Annotate its GeneID
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneID", value=interactor, type="cross-reference") )

        # Annotate its TaxID
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "TaxID", value=taxID, type="cross-reference") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[interactor] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_functional_association_entity(self, parser, interaction):
        """
        Create an external entity relation of a functional association and add it in BIANA
        """

        # CREATE THE EXTERNAL ENTITY RELATION
        # Create an external entity relation corresponding to a protein-protein interaction
        new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type="functional_association")


        # Add the interactors of the interaction as participants
        for interactor in interaction:
            new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[interactor] )


        # Add the sources used to report the interaction
        if interaction in parser.interaction_to_score:
            score = parser.interaction_to_score[interaction]
            new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "STRINGScore", value = score) )

        # Insert this external entity relation into database
        self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return






class STRINGnetwork(object):

    def __init__(self, input_file):

        self.input_file = input_file
        self.interactors = set()
        self.interactions = set()
        self.interaction_to_score = {}

        return



    def parse(self):

        with open(self.input_file, 'r') as input_fd:
            first_line = input_fd.readline()
            for line in input_fd:

                geneID1, geneID2, score = line.strip().split('\t')
                score = int(float(score) * 1000.0)

                interaction = frozenset([geneID1, geneID2])
                self.interactions.add(interaction)
                self.interactors.add(geneID1)
                self.interactors.add(geneID2)
                self.interaction_to_score[interaction] = score

        return



    def obtain_header_fields(self, first_line):
        """ 
        Obtain a dictionary: "field_name" => "position" 
        """
        fields_dict = {}

        header_fields = first_line.strip().split("\t")
        for x in xrange(0, len(header_fields)):
            fields_dict[header_fields[x]] = x

        return fields_dict

    def check_id(self, identifier, type_id):
        """ 
        Checks if there is an id 
        """
        if identifier.startswith(type_id):
            identifier = identifier.split(type_id+':')[1]
        else:
            print('Not {} --> {}'.format(type_id,identifier))
            sys.exit(10)

        return

    def analyze_identifier(self, identifier):
        """ 
        From the identifier of an interactor, we return the type of identifier and the value
        Example ---> entrez gene:1029 ---> 'entrez gene', '1029'
        """
        type_identifier, value_identifier = identifier.split(':')

        return type_identifier, value_identifier



if __name__ == "__main__":
    main()