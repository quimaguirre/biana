from bianaParser import *
                    
class hippieParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from the Hippie interactome

    """                 
                                                                                         
    name = "hippie"
    description = "This file implements a program that fills up tables in BIANA database from data in the Hippie Interactome"
    external_entity_definition = "An external entity represents a protein"
    external_entity_relations = "An external relation represents a interaction"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "Hippie interactome",  
                             default_script_name = "hippieParser.py",
                             default_script_description = hippieParser.description,     
                             additional_compulsory_arguments = [])
                    
    def parse_database(self):
        """                                                                              
        Method that implements the specific operations of a MyData formatted file
        """

        # Check that the input file exists
        if not os.path.isfile(self.input_file):
            raise ValueError("The input file is missing. Please, download the file \'HIPPIE-current.mitab.txt\'")


        #########################################################################
        #### NOTE: We are adding only the interactions that have reported methods
        #### The interactions without method are excluded.
        #########################################################################


        # Add HIPPIE_score as a valid external entity attribute since it is not recognized by BIANA
        self.biana_access.add_valid_external_entity_attribute_type( name = "HIPPIE_score",
                                                                    data_type = "varchar(20)",
                                                                    category = "eE identifier attribute")

        # Add HIPPIE_source as a valid external entity attribute since it is not recognized by BIANA
        self.biana_access.add_valid_external_entity_attribute_type( name = "HIPPIE_source",
                                                                    data_type = "varchar(100)",
                                                                    category = "eE identifier attribute")

        # Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
        self.biana_access.refresh_database_information()


        # Parse the database
        parser = Hippie_Interactome(self.input_file)
        parser.parse()


        print('The number of interactions is: {}'.format(len(parser.interactions)))


        # Defining the dict in which we will store the created external entities
        self.external_entity_ids_dict = {}

        #print(parser.interaction2score)
        #sys.exit(10)

        print("\n.....INSERTING THE INTERACTORS IN THE DATABASE.....\n")

        for interactor in parser.interactors:

            # Create an external entity corresponding to the interactor in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(interactor):

                #print("Adding interactor: {}".format(interactor))
                self.create_protein_external_entity(parser, interactor) 



        print("\n.....INSERTING THE INTERACTIONS IN THE DATABASE.....\n")

        for interaction in parser.interactions:

            #print("Adding interaction: {}".format(interaction))
            self.create_interaction_entity(parser, interaction)



        print("\nPARSING OF HIPPIE INTERACTOME FINISHED. THANK YOU FOR YOUR PATIENCE\n")

        return



    def create_protein_external_entity(self, parser, interactor):
        """
        Create an external entity of a protein and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        interactor_id = parser.interactor2id2altid[interactor]['id'] # Get the interactor id
        interactor_altid = parser.interactor2id2altid[interactor]['altid'] # Get the interactor alternative id

        for identifier in (interactor_id, interactor_altid):

            # Skip the empty cases
            if identifier == '-':
                continue

            type_id, value_id = parser.analyze_identifier(identifier)

            if type_id == 'entrez gene':

                # Annotate its GeneID
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneID", value=value_id, type="cross-reference") )

            elif type_id == 'uniprotkb':

                # Annotate its Uniprot Entry
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotEntry", value=value_id, type="cross-reference") )

            else:
                print('Unknown identifier: {}'.format(identifier))
                sys.exit(10)

            # Annotate its TaxID
            taxID = parser.interactor2taxid[interactor]
            if taxID != '-':
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
        for interactor in parser.interaction2interactor[interaction]:
            new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[interactor] )


        # Add the methods used to report the interaction
        methods = parser.interaction2methods[interaction]
        if methods != '-':
            for method in methods:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "method_id",
                                                                                                                     value = method) )
        else:
            print("Method not available for {}".format(interaction))
            sys.exit(10)


        # Add the pubmeds used to report the interaction
        pubmeds = parser.interaction2pubmeds[interaction]
        if pubmeds != '-':
            for pubmed in pubmeds:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "pubmed",
                                                                                                                     value = pubmed,
                                                                                                                     type="cross-reference") )
        else:
            print("Pubmed not available for {}".format(interaction))
            #sys.exit(10)

        # Add the sources used to report the interaction
        source_regex = re.compile('MI:[0-9]{4}\(([a-zA-Z]+)\)')
        sources = parser.interaction2sources[interaction]
        if sources != '-':
            for source in sources:
                m = source_regex.search(source)
                if m:
                    source = m.group(1)

                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "HIPPIE_source",
                                                                                                                     value = source,
                                                                                                                     type="cross-reference") )
        else:
            print("Source not available for {}".format(interaction))
            #sys.exit(10)

        score = parser.interaction2score[interaction]
        if score != '-':
            new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "HIPPIE_score",
                                                                                                                 value = score,
                                                                                                                 type="cross-reference") )
        else:
            print("Score not available for {}".format(interaction))
            sys.exit(10)


        # Insert this external entity relation into database
        self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return






class Hippie_Interactome(object):

    def __init__(self, input_file):

        self.hippie_file = input_file

        self.interactors = set()
        self.interactor2id2altid = {}
        self.interactor2taxid = {}

        self.interactions = set()
        self.interaction2interactor = {}
        self.interaction2methods = {}
        self.interaction2pubmeds = {}
        self.interaction2sources = {}
        self.interaction2score = {}

        return



    def parse(self):

        print("\n.....PARSING THE HIPPIE INTERACTOME.....\n")

        hippie_file_fd = open(self.hippie_file,'r')

        first_line = hippie_file_fd.readline()

        # Obtain a dictionary: "field_name" => "position"
        fields_dict = self.obtain_header_fields(first_line)
        #ID Interactor A    ID Interactor B Alt IDs Interactor A    Alt IDs Interactor B    Aliases Interactor A    Aliases Interactor B    Interaction Detection Methods   Publication 1st Author  Publication Identifiers Taxid Interactor A  Taxid Interactor B  Interaction Types   Source Databases    Interaction Identifiers Confidence Value    Presence In Other Species


        for line in hippie_file_fd:

            # Split the line in fields
            fields = line.strip().split("\t")

            # entrez gene:216 entrez gene:216 uniprotkb:AL1A1_HUMAN   uniprotkb:AL1A1_HUMAN   -   -   MI:0493(in vivo)|MI:0018(two hybrid)    Rodriguez-Zavala JS (2002)|Rual JF (2005)|Rolland T (2014)  pubmed:12081471|pubmed:16189514|pubmed:25416956 taxid:9606(Homo sapiens)    taxid:9606(Homo sapiens)    -   MI:0468(hprd)|biogrid|MI:0469(intact)|MI:0471(mint)|i2d|rual05  -   0.76    

            #### Obtain IDENTIFIERS ####

            # Obtain the fields of interest
            identifier1 = fields[ fields_dict['ID Interactor A'] ]
            identifier2 = fields[ fields_dict['ID Interactor B'] ]
            alt_ident1 = fields[ fields_dict['Alt IDs Interactor A'] ]
            alt_ident2 = fields[ fields_dict['Alt IDs Interactor B'] ]

            # If we do not have identifiers, we stop the parsing
            if identifier1 == '-' and alt_ident1 == '-':
                print('Identifier unknown')
                print('Identifier: {} Alt identifier: {}'.format(identifier1, alt_ident1))
                sys.exit(10)
            if identifier2 == '-' and alt_ident2 == '-':
                print('Identifier unknown')
                print('Identifier: {} Alt identifier: {}'.format(identifier2, alt_ident2))
                sys.exit(10)

            # If we do not have main identifier, we use the alternative as main
            if identifier1 == '-' and alt_ident1 != '-': # For interactor 1
                id1 = alt_ident1
                alt_id1 = identifier1
            else:
                id1 = identifier1
                alt_id1 = alt_ident1
            if identifier2 == '-' and alt_ident2 != '-': # For interactor 2
                id2 = alt_ident2
                alt_id2 = identifier2
            else:
                id2 = identifier2
                alt_id2 = alt_ident2

            #### Obtain TAXID ####

            taxid1 = fields[ fields_dict['Taxid Interactor A'] ]
            taxid2 = fields[ fields_dict['Taxid Interactor B'] ]
            self.check_id(taxid1, 'taxid')
            self.check_id(taxid2, 'taxid')
            taxid1 = taxid1[len('taxid:'):].split('(')[0] # Get only the taxonomy ID: taxid:9606(Homo sapiens) --> 9606
            taxid2 = taxid2[len('taxid:'):].split('(')[0] # Get only the taxonomy ID: taxid:9606(Homo sapiens) --> 9606

            #### Obtain METHOD ID ####

            if fields[ fields_dict['Interaction Detection Methods'] ] == '-' or fields[ fields_dict['Interaction Detection Methods'] ] == '':
#### ---------> IF THERE IS NO METHOD, WE SKIP THE INTERACTION!!! ######
                continue
            else:
                all_methods = fields[ fields_dict['Interaction Detection Methods'] ].split('|') # MI:0493(in vivo)|MI:0018(two hybrid)
                methods = []
                for method in all_methods:
                    if method.startswith('MI:'):
                        methods.append(method[len('MI:'):].split('(')[0]) # Get only the method ID
                    else:
                        print('Method does not start with MI: {}'.format(method))
                        #sys.exit(10)

            #### Obtain PUBMED ID ####

            if fields[ fields_dict['Publication Identifiers'] ] == '-' or fields[ fields_dict['Publication Identifiers'] ] == '':
                pubmeds = '-' 
            else:
                pubmeds = fields[ fields_dict['Publication Identifiers'] ].split('|') # pubmed:12081471|pubmed:16189514|pubmed:25416956
                [ self.check_id(x, 'pubmed') for x in pubmeds ]
                pubmeds = [ x[len('pubmed:'):] for x in pubmeds ] # Get only the pubmed ID

            #### Obtain SOURCE DATABASES ID ####

            if fields[ fields_dict['Source Databases'] ] == '-' or fields[ fields_dict['Source Databases'] ] == '':
                sources = '-'
                print('No source for ids {} {}'.format(id1, id2))
            else:
                sources = fields[ fields_dict['Source Databases'] ].split('|') # MI:0468(hprd)|biogrid|MI:0469(intact)|MI:0471(mint)|i2d|rual05

            #### Obtain CONFIDENCE VALUE ####

            if fields[ fields_dict['Confidence Value'] ] == '-':
                score = '-'
                print('No score')
                sys.exit(10)
            else:
                score = float(fields[ fields_dict['Confidence Value'] ])

            #### CREATE INTERACTION ID ####

            # Create an interaction id for the protein-protein interaction
            # ---> interaction id = identifier1 + '---' + geneid2
            interaction_1 = id1 + '---' + id2
            interaction_2 = id2 + '---' + id1
            interaction_3 = alt_id1 + '---' + alt_id2
            interaction_4 = alt_id2 + '---' + alt_id1

            #### CREATE INTERACTOR ID ####

            # We create an interactor id composed by the id + the alt_id
            interactor_1 = id1 + '---' + alt_id1
            interactor_2 = id2 + '---' + alt_id2

            #### INSERT THE FIELDS INTO DICTIONARIES

            # Check if interaction was already reported
            if not interaction_1 in self.interactions:
                interaction = interaction_1
            # If the identifier of the interaction is already used, we assign another one
            elif not interaction_2 in self.interactions:
                interaction = interaction_2
            elif not interaction_3 in self.interactions:
                interaction = interaction_3
            elif not interaction_4 in self.interactions:
                interaction = interaction_4
            # If all the possible identifiers are used, we stop... but this has not happened :)
            else:
                print('This interaction is already reported: {}'.format(interaction_1))
                print('The other way around was also reported: {}'.format(interaction_2))
                sys.exit(10)

            # Add the interaction
            self.interactions.add(interaction)

            # Add identifiers of the interaction
            self.interactors.add(interactor_1)
            self.interactors.add(interactor_2)

            self.interactor2id2altid.setdefault(interactor_1, {})
            self.interactor2id2altid[interactor_1]['id'] = id1
            self.interactor2id2altid[interactor_1]['altid'] = alt_id1

            self.interactor2id2altid.setdefault(interactor_2, {})
            self.interactor2id2altid[interactor_2]['id'] = id2
            self.interactor2id2altid[interactor_2]['altid'] = alt_id2

            self.interaction2interactor.setdefault(interaction, set())
            self.interaction2interactor[interaction].add(interactor_1)
            self.interaction2interactor[interaction].add(interactor_2)

            # Insert the taxIDs of the interactors
            self.interactor2taxid[interactor_1] = taxid1
            self.interactor2taxid[interactor_2] = taxid2

            # Insert methods of the interaction
            self.interaction2methods[interaction] = methods

            # Insert pubmeds of the interaction
            self.interaction2pubmeds[interaction] = pubmeds

            # Insert sources of the interaction
            self.interaction2sources[interaction] = sources

            # Insert score of the interaction
            self.interaction2score[interaction] = score

        hippie_file_fd.close()

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