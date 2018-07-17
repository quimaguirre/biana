from bianaParser import *
                    
class hitpredictParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from the HitPredict interactome

    """                 
                                                                                         
    name = "hitpredict"
    description = "This file implements a program that fills up tables in BIANA database from data in the HitPredict Interactome"
    external_entity_definition = "An external entity represents a protein"
    external_entity_relations = "An external relation represents a interaction"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "HitPredict interactome",  
                             default_script_name = "hitpredictParser.py",
                             default_script_description = hitpredictParser.description,     
                             additional_compulsory_arguments = [])
        self.default_eE_attribute = "uniprotentry"
                    
    def parse_database(self):
        """                                                                              
        Method that implements the specific operations of a MyData formatted file
        """

        # Check that the input file exists
        if not os.path.isfile(self.input_file):
            raise ValueError("The input file is missing. Please, introduce as input the file \'core.psimitab\'")


        #########################################################################
        #### NOTE: We are adding only the interactions that have reported methods
        #### The interactions without method are excluded.
        #########################################################################


        # Add HitPredict_score as a valid external entity attribute since it is not recognized by BIANA
        self.biana_access.add_valid_external_entity_attribute_type( name = "HitPredict_score",
                                                                    data_type = "varchar(20)",
                                                                    category = "eE identifier attribute")

        # Add HitPredict_source as a valid external entity attribute since it is not recognized by BIANA
        self.biana_access.add_valid_external_entity_attribute_type( name = "HitPredict_source",
                                                                    data_type = "varchar(100)",
                                                                    category = "eE identifier attribute")

        # Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
        self.biana_access.refresh_database_information()

        # Parse the database
        parser = HitPredict_Interactome(self.input_file)
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



        print("\nPARSING OF HITPREDICT INTERACTOME FINISHED. THANK YOU FOR YOUR PATIENCE\n")

        return



    def create_protein_external_entity(self, parser, interactor):
        """
        Create an external entity of a protein and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        interactor_id = parser.interactor2id2altid[interactor]['id'] # Get the interactor id
        interactor_altids = parser.interactor2id2altid[interactor]['altid'].split('|') # Get the interactor alternative id
        identifiers = [interactor_id]+interactor_altids

        for identifier in identifiers:

            # Skip the empty cases
            if identifier == '-':
                continue

            type_id, value_id = parser.analyze_identifier(identifier)

            if type_id == 'uniprotkb':

                if len(value_id.split('_')) == 2:
                    # Annotate its Uniprot Entry
                    new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotEntry", value=value_id, type="cross-reference") )
                elif len(value_id.split('_')) == 1:
                    # Annotate its Uniprot Accession
                    new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotAccession", value=value_id, type="cross-reference") )
                else:
                    print('Unknown uniprot ID: {}'.format(value_id))
                    sys.exit(10)

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
                                                                                                                     value = method,
                                                                                                                     type="cross-reference") )
        else:
            print("Method not available for {}".format(interaction))
            sys.exit(10)


        # Add the sources used to report the interaction
        #source_regex = re.compile('MI:[0-9]{4}\(([a-zA-Z]+)\)')
        sources = parser.interaction2sources[interaction]
        if sources != '-':
            for source in sources:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "HitPredict_source",
                                                                                                                     value = source,
                                                                                                                     type="unique") )
        else:
            print("Source not available for {}".format(interaction))
            #sys.exit(10)

        # Add the pubmeds used to report the interaction
        pubmeds = parser.interaction2pubmeds[interaction]
        if pubmeds != '-':
            for pubmed in pubmeds:
                if pubmed != '-':
                    new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "pubmed",
                                                                                                 value = pubmed,
                                                                                                 type="cross-reference") )
        else:
            print("Pubmed not available for {}".format(interaction))
            #sys.exit(10)

        scores = parser.interaction2score[interaction]
        for score in scores:
            type_score, score_value = score.split(':')
            if type_score == 'interaction-score':
                if score_value != '-':
                    new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "HitPredict_score",
                                                                                                 value = score_value,
                                                                                                 type="unique") )
                else:
                    print("Score not available for {}".format(interaction))
                    sys.exit(10)


        # Insert this external entity relation into database
        self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return






class HitPredict_Interactome(object):

    def __init__(self, input_file):

        self.core_file = input_file

        self.interactors = set()
        self.interactor2id2altid = {}
        self.interactor2taxid = {}

        self.interactions = set()
        self.interaction2interactor = {}
        self.interaction2methods = {}
        self.interaction2pubmeds = {}
        self.interaction2sources = {}
        self.interaction2score = {}

        self.type_ids = set()

        return



    def parse(self):

        print("\n.....PARSING THE HITPREDICT INTERACTOME.....\n")

        core_file_fd = open(self.core_file,'r')

        for line in core_file_fd:

            # Split the line in fields
            fields = line.strip().split("\t")

            # 1. ID1
            # 2. ID2
            # 3. Gene name1
            # 4. Gene name2
            # 5. -
            # 6. -
            # 7. Interaction methods
            # 8. -
            # 9. Pubmed
            # 10. TaxID 1
            # 11. TaxID 2
            # 12. Type of interaction
            # 13. Source
            # 14. ID of the interaction
            # 15. Scores
            # uniprotkb:Q9W4S7  uniprotkb:Q9VZF4    uniprotkb:MYC_DROME(gene name)  uniprotkb:FBXW7_DROME(gene name)    -   -   psi-mi:"MI:0007"(anti tag coimmunoprecipitation)    -   pubmed:15182669 taxid:7227(Drosophila melanogaster) taxid:7227(Drosophila melanogaster) psi-mi:"MI:0915"(physical association)  psi-mi:"MI:0469"(intact)    intact:EBI-490578   annotation-score:0.55|method-score:0.3819|interaction-score:0.458306665890864

            #### Obtain IDENTIFIERS ####

            # Obtain the fields of interest
            identifier1 = fields[0] # UniprotAccession OR UniprotEntry of Protein 1
            identifier2 = fields[1] # UniprotAccession OR UniprotEntry of Protein 2
            alt_ident1 = fields[2]
            alt_ident2 = fields[3]
            all_methods = fields[6]
            pubmeds = fields[8]
            taxid1 = fields[9]
            taxid2 = fields[10]
            type_interaction = fields[11]
            all_sources = fields[12]
            scores = fields[14]

            # If we do not have identifiers, we stop the parsing
            if identifier1 == '-' and alt_ident1 == '-':
                print('Identifier unknown')
                print('Identifier: {} Alt identifier: {}'.format(identifier1, alt_ident1))
                sys.exit(10)
            if identifier2 == '-' and alt_ident2 == '-':
                print('Identifier unknown')
                print('Identifier: {} Alt identifier: {}'.format(identifier2, alt_ident2))
                sys.exit(10)

            # If we do not have main identifier, we stop parsing
            if identifier1 == '-': # For interactor 1
                print('Identifier unknown')
                sys.exit(10)
            if identifier2 == '-': # For interactor 1
                print('Identifier unknown')
                sys.exit(10)

            #### Obtain TAXID ####

            self.check_id(taxid1, 'taxid')
            self.check_id(taxid2, 'taxid')
            taxid1 = taxid1[len('taxid:'):].split('(')[0] # Get only the taxonomy ID: taxid:9606(Homo sapiens) --> 9606
            taxid2 = taxid2[len('taxid:'):].split('(')[0] # Get only the taxonomy ID: taxid:9606(Homo sapiens) --> 9606

            #### Obtain METHOD ID ####

            if all_methods == '-' or all_methods == '':
#### ---------> IF THERE IS NO METHOD, WE SKIP THE INTERACTION!!! ######
                continue
            else:
                methods = []
                all_methods = all_methods.split('|') #psi-mi:"MI:0096"(pull down)|psi-mi:"MI:0676"(tandem affinity purification)
                for method in all_methods:
                    self.check_id(method, 'psi-mi')
                    method = method[len('psi-mi:'):].split('(')[0] # Get only the psimi: psi-mi:"MI:0045"(experimental interaction detection) --> "MI:0045"
                    if method.startswith('"MI:'):
                        methods.append(method[len('"MI:'):].split('"')[0]) # Get only the method ID from i.e. psi-mi:"MI:0045"(experimental interaction detection)
                    else:
                        print('Method does not start with MI: {}'.format(method))
                        sys.exit(10)

            #### Obtain PUBMED ID ####

            if pubmeds == '-' or pubmeds == '':
                pubmeds = '-' 
            else:
                pubmeds = pubmeds.split('|') # pubmed:15690043|pubmed:19402753
                [ self.check_id(x, 'pubmed') for x in pubmeds ]
                pubmeds = [ x[len('pubmed:'):] for x in pubmeds ] # Get only the pubmed ID

            #### Obtain SOURCE TYPE OF DATABASES ####

            if all_sources == '-' or all_sources == '':
                all_sources = '-'
                print('No source for ids {} {}'.format(identifier1, identifier2))
            else:
                sources = []
                all_sources = all_sources.split('|') # psi-mi:"MI:0461"(interaction database)
                for source in all_sources:
                    self.check_id(source, 'psi-mi')
                    source = source[len('psi-mi:'):] # Get only the psimi: psi-mi:"MI:0045"(experimental interaction detection) --> "MI:0045"(experimental interaction detection)
                    if source.startswith('"MI:'):
                        source = source.split('(')[1].split(')')[0]
                        sources.append(source) # Get only the source ID from i.e. psi-mi:"MI:0045"(experimental interaction detection)
                    else:
                        print('Source does not start with MI: {}'.format(source))
                        sys.exit(10)

            #### Obtain CONFIDENCE VALUE ####

            if scores == '-':
                scores = '-'
                print('No score')
                sys.exit(10)
            else:
                scores = scores.split('|')

            #### CREATE INTERACTION ID ####

            # Create an interaction id for the protein-protein interaction
            # ---> interaction id = identifier1 + '---' + geneid2
            interaction_1 = identifier1 + '---' + identifier2
            interaction_2 = identifier2 + '---' + identifier1

            #### CREATE INTERACTOR ID ####

            # We create an interactor id composed by the identifier
            interactor_1 = identifier1
            interactor_2 = identifier2
            if interactor_1.split(':')[1] == '' or interactor_2.split(':') == '':
                print('Empty interaction!')
                continue

            #### INSERT THE FIELDS INTO DICTIONARIES

            # Check if interaction was already reported
            if not interaction_1 in self.interactions or not interaction_2 in self.interactions:
                interaction = interaction_1
            else:
                print('This interaction is already reported: {}'.format(interaction_1))
                sys.exit(10)

            # Add the interaction
            self.interactions.add(interaction)

            # Add identifiers of the interaction
            self.interactors.add(interactor_1)
            self.interactors.add(interactor_2)

            # Process alt identifiers
            # uniprotkb:FBXW7_DROME(gene name)
            if len(alt_ident1.split('(')) == 2:
                alt_ident1 = alt_ident1.split('(')[0]
            if len(alt_ident2.split('(')) == 2:
                alt_ident2 = alt_ident2.split('(')[0]
            self.interactor2id2altid.setdefault(interactor_1, {})
            self.interactor2id2altid[interactor_1]['id'] = identifier1
            self.interactor2id2altid[interactor_1]['altid'] = alt_ident1

            self.interactor2id2altid.setdefault(interactor_2, {})
            self.interactor2id2altid[interactor_2]['id'] = identifier2
            self.interactor2id2altid[interactor_2]['altid'] = alt_ident2

            self.interaction2interactor.setdefault(interaction, set())
            self.interaction2interactor[interaction].add(interactor_1)
            self.interaction2interactor[interaction].add(interactor_2)

            # Insert the taxIDs of the interactors
            self.interactor2taxid[interactor_1] = taxid1
            self.interactor2taxid[interactor_2] = taxid2

            # Insert methods of the interaction
            self.interaction2methods.setdefault(interaction, set())
            for method in methods:
                self.interaction2methods[interaction].add(int(method))

            # Insert sources of the interaction
            self.interaction2sources.setdefault(interaction, set())
            for source in sources:
                self.interaction2sources[interaction].add(source)

            # Insert pubmeds of the interaction
            self.interaction2pubmeds.setdefault(interaction, set())
            for pubmed in pubmeds:
                if pubmed.startswith('unassigned'):
                    self.interaction2pubmeds[interaction].add('-')
                else:
                    self.interaction2pubmeds[interaction].add(pubmed)

            # Insert score of the interaction
            self.interaction2score[interaction] = scores

            self.type_ids.add(self.analyze_identifier(identifier1)[0])
            self.type_ids.add(self.analyze_identifier(identifier2)[0])
            for alt_id in alt_ident1.split("|"):
                self.type_ids.add(self.analyze_identifier(alt_id)[0])
            for alt_id in alt_ident2.split("|"):
                self.type_ids.add(self.analyze_identifier(alt_id)[0])

        core_file_fd.close()

        print(self.type_ids)

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