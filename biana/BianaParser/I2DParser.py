from bianaParser import *
                    
class I2DParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from the I2D interactome

    """                 
                                                                                         
    name = "i2d"
    description = "This file implements a program that fills up tables in BIANA database from data in the I2D Interactome"
    external_entity_definition = "An external entity represents a protein"
    external_entity_relations = "An external relation represents a interaction"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "I2D interactome",  
                             default_script_name = "I2DParser.py",
                             default_script_description = I2DParser.description,     
                             additional_compulsory_arguments = [])
        self.default_eE_attribute = "uniprotaccession"
                    
    def parse_database(self):
        """                                                                              
        Method that implements the specific operations of a MyData formatted file
        """

        self.biana_access.add_valid_external_entity_attribute_type( name = "I2D_source",
                                                                    data_type = "varchar(100)",
                                                                    category = "eE identifier attribute")

        # Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
        self.biana_access.refresh_database_information()



        ###########################
        #### CHECK INPUT FILES ####
        ###########################

        # Check that all the files exist
        if os.path.isdir(self.input_file):
            self.input_path = self.input_file
        else:
            raise ValueError("You must specify a path instead of a file")



        ########################
        #### PARSE DATABASE ####
        ########################

        species_to_taxID = {
            'human' : 9606,
            'mouse' : 10090,
            'rat'   : 10116,
            'worm'  : 6239,
            'yeast' : 4932,
            'fly'   : 7227,
            'hhv8'  : 37296
        }

        # Parse the human interactome
        print("\n.....PARSING THE I2D HUMAN INTERACTOME.....\n")
        human_file = os.path.join(self.input_path, 'i2d.2_9.Public.HUMAN.tab')
        parser_human = I2D(human_file, 'human')
        parser_human.parse()
        print('{} interactions'.format(len(parser_human.interactions)))

        # Parse the mouse interactome
        print("\n.....PARSING THE I2D MOUSE INTERACTOME.....\n")
        mouse_file = os.path.join(self.input_path, 'i2d.2_9.Public.MOUSE.tab')
        parser_mouse = I2D(mouse_file, 'mouse')
        parser_mouse.parse()
        print('{} interactions'.format(len(parser_mouse.interactions)))

        # Parse the rat interactome
        print("\n.....PARSING THE I2D RAT INTERACTOME.....\n")
        rat_file = os.path.join(self.input_path, 'i2d.2_9.Public.RAT.tab')
        parser_rat = I2D(rat_file, 'rat')
        parser_rat.parse()
        print('{} interactions'.format(len(parser_rat.interactions)))

        # Parse the worm interactome
        print("\n.....PARSING THE I2D WORM INTERACTOME.....\n")
        worm_file = os.path.join(self.input_path, 'i2d.2_9.Public.WORM.tab')
        parser_worm = I2D(worm_file, 'worm')
        parser_worm.parse()
        print('{} interactions'.format(len(parser_worm.interactions)))

        # Parse the yeast interactome
        print("\n.....PARSING THE I2D YEAST INTERACTOME.....\n")
        yeast_file = os.path.join(self.input_path, 'i2d.2_9.Public.YEAST.tab')
        parser_yeast = I2D(yeast_file, 'yeast')
        parser_yeast.parse()
        print('{} interactions'.format(len(parser_yeast.interactions)))

        # Parse the fly interactome
        print("\n.....PARSING THE I2D FLY INTERACTOME.....\n")
        fly_file = os.path.join(self.input_path, 'i2d.2_9.Public.FLY.tab')
        parser_fly = I2D(fly_file, 'fly')
        parser_fly.parse()
        print('{} interactions'.format(len(parser_fly.interactions)))

        # Parse the HHV8 interactome
        print("\n.....PARSING THE I2D HHV8 INTERACTOME.....\n")
        hhv8_file = os.path.join(self.input_path, 'i2d.2_9.Public.HHV8.tab')
        parser_hhv8 = I2D(hhv8_file, 'hhv8')
        parser_hhv8.parse()
        print('{} interactions'.format(len(parser_hhv8.interactions)))



        #########################################
        #### INSERT PARSED DATABASE IN BIANA ####
        #########################################

        # Defining the dict in which we will store the created external entities
        self.external_entity_ids_dict = {}


        print("\n.....INSERTING THE INTERACTORS IN THE DATABASE.....\n")

        # For HUMAN
        for interactor in parser_human.interactors:
            if not self.external_entity_ids_dict.has_key(interactor):
                self.create_protein_external_entity(parser_human, interactor, species_to_taxID[parser_human.species]) 

        # For MOUSE
        for interactor in parser_mouse.interactors:
            if not self.external_entity_ids_dict.has_key(interactor):
                self.create_protein_external_entity(parser_mouse, interactor, species_to_taxID[parser_mouse.species]) 

        # For RAT
        for interactor in parser_rat.interactors:
            if not self.external_entity_ids_dict.has_key(interactor):
                self.create_protein_external_entity(parser_rat, interactor, species_to_taxID[parser_rat.species]) 

        # For WORM
        for interactor in parser_worm.interactors:
            if not self.external_entity_ids_dict.has_key(interactor):
                self.create_protein_external_entity(parser_worm, interactor, species_to_taxID[parser_worm.species]) 

        # For YEAST
        for interactor in parser_yeast.interactors:
            if not self.external_entity_ids_dict.has_key(interactor):
                self.create_protein_external_entity(parser_yeast, interactor, species_to_taxID[parser_yeast.species]) 

        # For FLY
        for interactor in parser_fly.interactors:
            if not self.external_entity_ids_dict.has_key(interactor):
                self.create_protein_external_entity(parser_fly, interactor, species_to_taxID[parser_fly.species]) 

        # For HHV8
        for interactor in parser_hhv8.interactors:
            if not self.external_entity_ids_dict.has_key(interactor):
                self.create_protein_external_entity(parser_hhv8, interactor, species_to_taxID[parser_hhv8.species]) 


        print("\n.....INSERTING THE INTERACTIONS IN THE DATABASE.....\n")

        # For HUMAN
        for interaction in parser_human.interactions:
            self.create_interaction_entity(parser_human, interaction)

        # For MOUSE
        for interaction in parser_mouse.interactions:
            self.create_interaction_entity(parser_mouse, interaction)

        # For RAT
        for interaction in parser_rat.interactions:
            self.create_interaction_entity(parser_rat, interaction)

        # For WORM
        for interaction in parser_worm.interactions:
            self.create_interaction_entity(parser_worm, interaction)

        # For YEAST
        for interaction in parser_yeast.interactions:
            self.create_interaction_entity(parser_yeast, interaction)

        # For FLY
        for interaction in parser_fly.interactions:
            self.create_interaction_entity(parser_fly, interaction)

        # For HHV8
        for interaction in parser_hhv8.interactions:
            self.create_interaction_entity(parser_hhv8, interaction)


        print("\nPARSING OF I2D INTERACTOME FINISHED. THANK YOU FOR YOUR PATIENCE\n")

        return



    def create_protein_external_entity(self, parser, interactor, taxID):
        """
        Create an external entity of a protein and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        # Annotate its UniprotAccession
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotAccession", value=interactor, type="cross-reference") )

        # Annotate its TaxID
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "TaxID", value=taxID, type="cross-reference") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[interactor] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_interaction_entity(self, parser, interaction):
        """
        Create an external entity relation of an interaction and add it in BIANA
        """

        # CREATE THE EXTERNAL ENTITY RELATION
        # Create an external entity relation corresponding to a protein-protein interaction
        new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "interaction" )


        # Add the interactors of the interaction as participants
        for interactor in interaction:
            new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[interactor] )


        # Add the sources used to report the interaction
        if interaction in parser.interaction_to_sources:
            sources = parser.interaction_to_sources[interaction]
            for source in sources:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "I2D_source",
                                                                                                                     value = source,
                                                                                                                     type="unique") )

        # Insert this external entity relation into database
        self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return






class I2D(object):

    def __init__(self, input_file, species):

        self.input_file = input_file
        self.species = species.lower()

        self.interactors = set()
        self.interactions = set()
        self.interaction_to_sources = {}

        return



    def parse(self):

        with open(self.input_file, 'r') as I2D_fd:
            first_line = I2D_fd.readline()
            for line in I2D_fd:

                src, node1, node2 = line.strip().split('\t')

                interaction = frozenset([node1, node2])
                self.interactions.add(interaction)
                self.interactors.add(node1)
                self.interactors.add(node2)
                self.interaction_to_sources.setdefault(interaction, set())
                self.interaction_to_sources[interaction].add(src.lower())

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