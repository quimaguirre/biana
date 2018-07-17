from bianaParser import *
                    
class ConsensusPathDBParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from the ConsensusPathDB interactome

    """                 
                                                                                         
    name = "consensuspathdb"
    description = "This file implements a program that fills up tables in BIANA database from data in the ConsensusPathDB Interactome"
    external_entity_definition = "An external entity represents a protein"
    external_entity_relations = "An external relation represents a interaction"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "ConsensusPathDB interactome",  
                             default_script_name = "ConsensusPathDBParser.py",
                             default_script_description = ConsensusPathDBParser.description,     
                             additional_compulsory_arguments = [])
        self.default_eE_attribute = "uniprotentry"
                    
    def parse_database(self):
        """                                                                              
        Method that implements the specific operations of a MyData formatted file
        """

        self.biana_access.add_valid_external_entity_attribute_type( name = "ConsensusPathDB_score",
                                                                    data_type = "varchar(20)",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "ConsensusPathDB_source",
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
        print("\n.....PARSING THE ConsensusPathDB HUMAN INTERACTOME.....\n")
        human_file = os.path.join(self.input_path, 'ConsensusPathDB_human_PPI')
        parser_human = ConsensusPathDB(human_file, 'human')
        parser_human.parse()

        # Parse the mouse interactome
        print("\n.....PARSING THE ConsensusPathDB MOUSE INTERACTOME.....\n")
        mouse_file = os.path.join(self.input_path, 'ConsensusPathDB_mouse_PPI')
        parser_mouse = ConsensusPathDB(mouse_file, 'mouse')
        parser_mouse.parse()

        # Parse the yeast interactome
        print("\n.....PARSING THE ConsensusPathDB YEAST INTERACTOME.....\n")
        yeast_file = os.path.join(self.input_path, 'ConsensusPathDB_yeast_PPI')
        parser_yeast = ConsensusPathDB(yeast_file, 'yeast')
        parser_yeast.parse()



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

        # For YEAST
        for interactor in parser_yeast.interactors:
            if not self.external_entity_ids_dict.has_key(interactor):
                self.create_protein_external_entity(parser_yeast, interactor, species_to_taxID[parser_yeast.species]) 


        print("\n.....INSERTING THE INTERACTIONS IN THE DATABASE.....\n")

        # For HUMAN
        for interaction in parser_human.interactions:
            self.create_interaction_entity(parser_human, interaction)

        # For MOUSE
        for interaction in parser_mouse.interactions:
            self.create_interaction_entity(parser_mouse, interaction)

        # For YEAST
        for interaction in parser_yeast.interactions:
            self.create_interaction_entity(parser_yeast, interaction)


        print("\nPARSING OF ConsensusPathDB INTERACTOME FINISHED. THANK YOU FOR YOUR PATIENCE\n")

        return



    def create_protein_external_entity(self, parser, interactor, taxID):
        """
        Create an external entity of a protein and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        # Annotate its UniprotEntry
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotEntry", value=interactor, type="cross-reference") )

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
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "ConsensusPathDB_source",
                                                                                                                     value = source,
                                                                                                                     type="unique") )

        # Add the pubmeds
        if interaction in parser.interaction_to_pubmeds:
            pubmeds = parser.interaction_to_pubmeds[interaction]
            for pubmed in pubmeds:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "Pubmed",
                                                                                                                     value = pubmed,
                                                                                                                     type="cross-reference") )

        # Add score
        if interaction in parser.interaction_to_score:
            score = parser.interaction_to_score[interaction]
            new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "ConsensusPathDB_score",
                                                                                                                 value = score,
                                                                                                                 type="unique") )

        # Insert this external entity relation into database
        self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return






class ConsensusPathDB(object):

    def __init__(self, input_file, species):

        self.input_file = input_file
        self.species = species.lower()

        self.interactors = set()
        self.interactions = set()
        self.interaction_to_score = {}
        self.interaction_to_pubmeds = {}
        self.interaction_to_sources = {}

        return



    def parse(self):

        with open(self.input_file, 'r') as input_fd:
            for line in input_fd:

                # Skip first lines
                if line[0] == '#':
                    continue

                # Get fields
                # source_databases interaction_publications    interaction_participants    interaction_confidence
                srcs, pubmeds, participants, score = line.strip().split('\t')

                # Get participants
                participants = participants.split(',')

                # Get the score
                if score == 'NA':
                    if self.species == 'human' or self.species == 'mouse':
                        continue
                    else:
                        pass

                else:
                    score = float(score)

                # Get pubmeds
                if pubmeds == '-' or pubmeds == '':
                    print('No pubmed for ids {}'.format(participants))
                else:
                    pubmeds = pubmeds.split(',')

                # Get sources
                if srcs == '-' or srcs == '':
                    print('No source for ids {}'.format(participants))
                    sys.exit(10)
                else:
                    sources = srcs.lower().split(',')

                # Get the interactions from the participants
                current_interactions = set()
                if len(participants) == 1:
                    interaction = frozenset([participants[0],participants[0]])
                    current_interactions.add(interaction)
                elif len(participants) == 2:
                    interaction = frozenset([participants[0],participants[1]])
                    current_interactions.add(interaction)
                # SKIP COMPLEXES!!!
                elif len(participants) > 2:
                    continue
                    # for protein1 in participants:
                    #     for protein2 in participants:
                    #         if protein1 != protein2:
                    #             interaction = frozenset([ protein1, protein2 ])
                    #             current_interactions.add(interaction)

                # Save the interactors
                for participant in participants:
                    self.interactors.add(participant)

                # Save the interactions and its information associated
                for interaction in current_interactions:
                    self.interactions.add(interaction)
                    self.interaction_to_score[interaction] = score
                    for pubmed in pubmeds:
                        # Skip pubmeds not valid
                        if 'unassigned' in pubmed:
                            continue
                        self.interaction_to_pubmeds.setdefault(interaction, set())
                        self.interaction_to_pubmeds[interaction].add(pubmed)
                    for source in sources:
                        self.interaction_to_sources.setdefault(interaction, set())
                        self.interaction_to_sources[interaction].add(source)

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