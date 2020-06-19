from bianaParser import *
                    
class InnateDBParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from the InnateDB

    """                 
                                                                                         
    name = "InnateDB"
    description = "This file implements a program that fills up tables in BIANA database from data in InnateDB"
    external_entity_definition = "An external entity represents a protein"
    external_entity_relations = "An external relation represents a interaction"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "InnateDB",  
                             default_script_name = "InnateDBParser.py",
                             default_script_description = InnateDBParser.description,     
                             additional_compulsory_arguments = [])
        self.default_eE_attribute = "innatedb"

    def parse_database(self):
        """
        Method that implements the specific operations in the InnateDB file.
        """

        # Check that the input file exists
        if not os.path.isfile(self.input_file):
            raise ValueError("The input file is missing. Please, introduce as input the file \'core.psimitab\'")


        #########################################################################
        #### NOTE: We are adding only the interactions that have reported methods
        #### The interactions without method are excluded.
        #########################################################################


        # Add InnateDB as a valid external entity attribute since it is not recognized by BIANA
        self.biana_access.add_valid_external_entity_attribute_type( name = "InnateDB",
                                                                    data_type = "varchar(20)",
                                                                    category = "eE identifier attribute")

        # Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
        self.biana_access.refresh_database_information()

        # Parse the database
        parser = InnateDB(self.input_file)
        parser.parse()


        print('The number of interactions is: {}'.format(len(parser.interactions)))


        # Defining the dict in which we will store the created external entities
        self.external_entity_ids_dict = {}


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



        print("\nPARSING OF INNATEDB FINISHED. THANK YOU FOR YOUR PATIENCE\n")

        return



    def create_protein_external_entity(self, parser, interactor):
        """
        Create an external entity of a protein and add it in BIANA
        """

        if parser.interactor2type[interactor] == 'protein':
            new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )
        elif parser.interactor2type[interactor] == 'deoxyribonucleic acid':
            new_external_entity = ExternalEntity( source_database = self.database, type = "dna" )
        elif parser.interactor2type[interactor] == 'ribonucleic acid':
            new_external_entity = ExternalEntity( source_database = self.database, type = "rna" )
        else:
            print('Unknown type of interactor for {}: {}'.format(interactor, parser.interactor2type[interactor]))
            sys.exit(10)

        # Annotate the InnateDB identifier
        parser.check_id(interactor, 'innatedb')
        type_id, value_id = parser.analyze_identifier(interactor)
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "InnateDB", value=value_id, type="unique") )

        # Annotate the alternative identifiers
        #set(['uniprotkb', 'mgi', 'ensembl', 'refseq', 'hgnc'])
        interactor_altids = list(parser.interactor2altids[interactor])
        for identifier in interactor_altids:

            # Skip the empty cases
            if identifier == '-':
                continue

            type_id, value_id = parser.analyze_identifier(identifier)

            if type_id == 'ensembl':
                # Annotate its Ensembl
                if value_id[0:3].upper() == 'ENS':
                    new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Ensembl", value=value_id, type="cross-reference") )

            elif type_id == 'uniprotkb':
                if len(value_id.split('_')) == 2:
                    # Annotate its Uniprot Entry
                    new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotEntry", value=value_id, type="cross-reference") )
                elif len(value_id.split('_')) == 1:
                    # Annotate its Uniprot Accession
                    new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotAccession", value=value_id, type="cross-reference") )
                else:
                    print('Unknown uniprot ID: {}'.format(value_id))
                    sys.exit(10)

            elif type_id == 'refseq':
                # Annotate its RefSeq
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "RefSeq", value=value_id, type="cross-reference") )

            elif type_id == 'hgnc':
                continue

            elif type_id == 'mgi':
                continue

            elif type_id == 'innatedb':
                continue

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
        interactor1 = parser.interaction2interactor1[interaction]
        interactor2 = parser.interaction2interactor2[interaction]
        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[interactor1] )
        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[interactor2] )

        # Add the InnateDB identifier of the interaction
        type_id, value_id = parser.analyze_identifier(interaction)
        if type_id.lower() == 'innatedb' or type_id.lower() == 'innatedb allergy':
            new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "InnateDB", value = value_id, type = "unique") )
        else:
            print('Unknown interaction identifier: {}'.format(interaction))
            sys.exit(10)

        # Add the methods used to report the interaction
        methods = parser.interaction2methods[interaction]
        if methods != '-':
            for method in methods:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "method_id", value = method) )
        else:
            print("Method not available for {}".format(interaction))
            sys.exit(10)

        # Add the methods used to report the interaction
        pubmeds = parser.interaction2pubmeds[interaction]
        if pubmeds != '-':
            for pubmed in pubmeds:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "Pubmed", value = pubmed, type = "cross-reference") )
        else:
            print("Pubmeds not available for {}".format(interaction))
            sys.exit(10)

        # Insert this external entity relation into database
        self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return






class InnateDB(object):

    def __init__(self, input_file):

        self.input_file = input_file

        self.interactors = set()
        self.interactor2type = {}
        self.interactor2altids = {}
        self.interactor2taxid = {}

        self.interactions = set()
        self.interaction2interactor1 = {}
        self.interaction2interactor2 = {}
        self.interaction2methods = {}
        self.interaction2pubmeds = {}
        self.interaction2sources = {}
        self.interaction2type = {}
        self.interaction2score = {}

        self.type_ids = set()
        self.interaction_sources = set()
        self.interaction_types = {}
        self.interactor_types = {}

        return



    def parse(self):

        print("\n.....PARSING THE INNATEDB INTERACTOME.....\n")

        with open(self.input_file,'r') as input_file_fd:

            first_line = input_file_fd.readline()

            for line in input_file_fd:

                # Split the line in fields
                fields = line.strip().split("\t")

                #0.unique_identifier_A    
                #1.unique_identifier_B 
                #2.alt_identifier_A    
                #3.alt_identifier_B    
                #4.alias_A 
                #5.alias_B 
                #6.interaction_detection_method    
                #7.author  
                #8.pmid    
                #9.ncbi_taxid_A
                #10ncbi_taxid_B    
                #11.interaction_type    
                #12.source_database 
                #13.idinteraction_in_source_db  
                #14.confidence_score    
                #15.expansion_method    
                #16.biological_role_A   
                #17.biological_role_B   
                #18.exp_role_A  
                #19.exp_role_B  
                #20.interactor_type_A   
                #21.interactor_type_B   
                #22.xrefs_A 
                #23.xrefs_B 
                #24.xrefs_interaction   
                #25.annotations_A   
                #26.annotations_B   
                #27.annotations_interaction 
                #28.ncbi_taxid_host_organism    
                #29.parameters_interaction  
                #30.creation_date   
                #update_date 
                #checksum_A  
                #checksum_B  
                #checksum_interaction    
                #negative    
                #features_A  
                #features_B  
                #stoichiometry_A 
                #stoichiometry_B 
                #participant_identification_method_A 
                #participant_identification_method_B

                # innatedb:IDBG-25842   innatedb:IDBG-82738 ensembl:ENSG00000154589 ensembl:ENSG00000136869 uniprotkb:LY96_HUMAN|refseq:NP_056179|uniprotkb:Q9Y6Y9|refseq:NP_001182726|hgnc:LY96(display_short) refseq:NP_612564|refseq:NP_612567|uniprotkb:O00206|uniprotkb:TLR4_HUMAN|refseq:NP_003257|hgnc:TLR4(display_short)   psi-mi:"MI:0007"(anti tag coimmunoprecipitation)    Shimazu et al. (1999)   pubmed:10359581 taxid:9606(Human)   taxid:9606(Human)   psi-mi:"MI:0915"(physical association)  MI:0974(innatedb)   innatedb:IDB-113240 lpr:3|hpr:3|np:1|   -   psi-mi:"MI:0499"(unspecified role)  psi-mi:"MI:0499"(unspecified role)  psi-mi:"MI:0498"(prey)  psi-mi:"MI:0496"(bait)  psi-mi:"MI:0326"(protein)   psi-mi:"MI:0326"(protein)   -   -   -   -   -   -   taxid:10090 -   2008/03/30  2008/03/30  -   -   -   false   -   -   -   -   psi-mi:"MI:0363"(inferred by author)    psi-mi:"MI:0363"(inferred by author)


                #### Obtain IDENTIFIERS ####

                # Obtain the fields of interest
                identifier1 = fields[0] # InnateDBID
                identifier2 = fields[1] # InnateDBID
                alt_ident1 = fields[2] # Ensembl
                alt_ident2 = fields[3] # Ensembl
                aliases1 = fields[4].split('|') # cross-references i.e. refseq:NP_002459|refseq:NP_001166039|refseq:NP_001166038|uniprotkb:MYD88_HUMAN|refseq:NP_001166040|uniprotkb:Q99836|refseq:NP_001166037|hgnc:MYD88(display_short)
                aliases2 = fields[5].split('|')
                all_methods = fields[6]
                all_pubmeds = fields[8]
                taxid1 = fields[9]
                taxid2 = fields[10]
                interaction_type = fields[11]
                all_sources = fields[12]
                interaction_id = fields[13]
                score = fields[14]
                interactor_type1 = fields[20]
                interactor_type2 = fields[21]

                # If we do not have main identifier, we stop parsing
                if identifier1 == '-': # For interactor 1
                    print('Identifier 1 unknown')
                    sys.exit(10)
                if identifier2 == '-': # For interactor 1
                    print('Identifier 2 unknown')
                    sys.exit(10)

                # If we do not have interaction identifier, we stop parsing
                if interaction_id == '-':
                    print('Interaction identifier unknown')
                    sys.exit(10)

                #### Obtain TAXID ####
                self.check_id(taxid1, 'taxid')
                self.check_id(taxid2, 'taxid')
                taxid1 = int(taxid1[len('taxid:'):].split('(')[0]) # Get only the taxonomy ID: taxid:9606(Human) --> 9606
                taxid2 = int(taxid2[len('taxid:'):].split('(')[0]) # Get only the taxonomy ID: taxid:9606(Human) --> 9606


                #### Obtain METHOD ID ####
#### ---------> IF THERE IS NO METHOD, WE SKIP THE INTERACTION!!! ######
                if all_methods == '-' or all_methods == '':
                    continue
                else:
                    methods = []
                    all_methods = all_methods.split('|')
                    for method in all_methods:
                        self.check_id(method, 'psi-mi')
                        method = method[len('psi-mi:'):].split('(')[0] # psi-mi:"MI:0007"(anti tag coimmunoprecipitation) --> "MI:0007"
                        if method.startswith('"MI:'):
                            method = int(method[len('"MI:'):].split('"')[0]) # "MI:0007" --> 7
                            methods.append(method)
                        else:
                            print('Method does not start with MI: {}'.format(method))
                            sys.exit(10)


                #### Obtain PUBMED IDS ####
                pubmeds = []
                if all_pubmeds == '-' or all_pubmeds == '':
                    print('NO PUBMED!')
                    sys.exit(10)
                else:
                    all_pubmeds = all_pubmeds.split('|')
                    for pubmed in all_pubmeds:
                        self.check_id(pubmed, 'pubmed')
                        pubmed = int(pubmed[len('pubmed:'):]) # pubmed:10359581 --> 10359581
                        pubmeds.append(pubmed)


                #### Obtain SOURCE TYPE OF DATABASES ####
                if all_sources == '-' or all_sources == '':
                    all_sources = '-'
                    print('No source for ids {} {}'.format(identifier1, identifier2))
                else:
                    sources = []
                    all_sources = all_sources.split('|')
                    for source in all_sources:
                        self.check_id(source, 'MI')
                        source = source.split('(')[1] # MI:0974(innatedb) --> innatedb)
                        source = source.split(')')[0] # --> innatedb
                        sources.append(source)
                        if source.lower() != 'innatedb':
                            print('Source unknown {} in interaction {}'.format(source, interaction_id))
                            sys.exit(10)


                #### Obtain TYPE OF INTERACTION ####
                if interaction_type == '-' or interaction_type == '':
                    all_sources = '-'
                    print('No interactor type for interaction {}'.format(interaction_id))
                    sys.exit(10)
                else:
                    # self.check_id(interaction_type, 'psi-mi')
                    # interaction_type = interaction_type[len('psi-mi:'):].split('(')[0] # psi-mi:"MI:0915"(physical association) --> "MI:0915"
                    # if interaction_type.startswith('"MI:'):
                    #     interaction_type = int(interaction_type[len('"MI:'):].split('"')[0]) # "MI:0915" --> 915
                    #     if interaction_type != 915:
                    #         print('Interaction type is not a physical association (915): {}'.format(interaction_type))
                    #         sys.exit(10)
                    # else:
                    #     print('Interaction type does not start with MI: {}'.format(interaction_type))
                    #     sys.exit(10)
                    self.check_id(interaction_type, 'psi-mi')
                    interaction_type = interaction_type.split('(')[1] # psi-mi:"MI:0915"(physical association) --> physical association)
                    interaction_type = interaction_type.split(')')[0]
                    if interaction_type != 'physical association':
#### -------------> IF THE TYPE OF INTERACTION IS NOT "PHYSICAL ASSOCIATION", WE SKIP THE INTERACTION!!! ######
                        print('Interaction type is not physical association, it is: {}'.format(interaction_type))
                        continue


                #### Obtain TYPE OF INTERACTOR ####
                if interactor_type1 == '-' or interactor_type1 == '':
                    print('No type for interactor {}'.format(identifier1))
                    sys.exit(10)
                else:
                    self.check_id(interactor_type1, 'psi-mi')
                    interactor_type1 = interactor_type1.split('(')[1] # psi-mi:"MI:0319"(deoxyribonucleic acid) -->deoxyribonucleic acid)
                    interactor_type1 = interactor_type1.split(')')[0]
                if interactor_type2 == '-' or interactor_type2 == '':
                    print('No type for interactor {}'.format(identifier2))
                    sys.exit(10)
                else:
                    self.check_id(interactor_type2, 'psi-mi')
                    interactor_type2 = interactor_type2.split('(')[1] # psi-mi:"MI:0319"(deoxyribonucleic acid) -->deoxyribonucleic acid)
                    interactor_type2 = interactor_type2.split(')')[0]



                # Define the interaction ID
                interaction = interaction_id

                # Add the interaction
                self.interactions.add(interaction)

                # Add identifiers of the interactors
                self.interactors.add(identifier1)
                self.interactors.add(identifier2)
                self.interaction2interactor1[interaction] = identifier1
                self.interaction2interactor2[interaction] = identifier2

                # Add the alternative identifiers of the interactors
                self.interactor2altids.setdefault(identifier1, set()).add(alt_ident1)
                self.interactor2altids.setdefault(identifier2, set()).add(alt_ident2)
                for alias in aliases1:
                    self.interactor2altids.setdefault(identifier1, set()).add(alias)
                for alias in aliases2:
                    self.interactor2altids.setdefault(identifier2, set()).add(alias)

                # Insert the taxIDs of the interactors
                self.interactor2taxid[identifier1] = taxid1
                self.interactor2taxid[identifier2] = taxid2

                # Insert methods of the interaction
                for method in methods:
                    self.interaction2methods.setdefault(interaction, set()).add(method)

                # Insert pubmeds of the interaction
                for pubmed in pubmeds:
                    self.interaction2pubmeds.setdefault(interaction, set()).add(pubmed)

                # Insert sources of the interaction
                for source in sources:
                    self.interaction2sources.setdefault(interaction, set()).add(source)
                    self.interaction_sources.add(source)

                # Insert the type of interaction
                self.interaction2type[interaction] = interaction_type
                self.interaction_types.setdefault(interaction_type, 0)
                self.interaction_types[interaction_type]+=1

                # Insert the type of interactors
                self.interactor2type[identifier1] = interactor_type1
                self.interactor2type[identifier2] = interactor_type2
                self.interactor_types.setdefault(interactor_type1, 0)
                self.interactor_types[interactor_type1]+=1
                self.interactor_types.setdefault(interactor_type2, 0)
                self.interactor_types[interactor_type2]+=1

                # Get the types of identifiers
                self.type_ids.add(self.analyze_identifier(identifier1)[0])
                self.type_ids.add(self.analyze_identifier(identifier2)[0])
                for alias in self.interactor2altids[identifier1]:
                    self.type_ids.add(self.analyze_identifier(alias)[0])
                for alias in self.interactor2altids[identifier2]:
                    self.type_ids.add(self.analyze_identifier(alias)[0])


        print(self.type_ids)
        #set(['uniprotkb', 'mgi', 'innatedb', 'ensembl', 'refseq', 'hgnc'])
        print(self.interaction_types)
        #'physical association': 31086
        #'phosphorylation reaction': 1201 
        #'ubiquitination reaction': 423
        #'protein cleavage': 324
        #'cleavage reaction': 147 
        #'dephosphorylation reaction': 40 
        #'other modification': 14
        #'sumoylation': 9
        #'disulfide bond': 8 
        #'deubiquitination reaction': 7
        #'methylation reaction': 6, 'biochemical': 6, 'enzymatic reaction': 6
        #'acetylation reaction': 5, 'covalent binding': 5
        #'sumoylation reaction': 3
        #'self interaction': 2, 'association': 2
        #'adp ribosylation reaction': 1
        print(self.interactor_types)
        #{'deoxyribonucleic acid': 8019, 'protein': 57445, 'ribonucleic acid': 1126}

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