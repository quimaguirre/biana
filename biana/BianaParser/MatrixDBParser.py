from bianaParser import *
                    
class MatrixDBParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from the MatrixDB

    """                 
                                                                                         
    name = "MatrixDB"
    description = "This file implements a program that fills up tables in BIANA database from data in MatrixDB"
    external_entity_definition = "An external entity represents a protein"
    external_entity_relations = "An external relation represents a interaction"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "MatrixDB",  
                             default_script_name = "MatrixDBParser.py",
                             default_script_description = MatrixDBParser.description,     
                             additional_compulsory_arguments = [])
        self.default_eE_attribute = "chebi"

    def parse_database(self):
        """
        Method that implements the specific operations in the MatrixDB file.
        """

        # Check that the input file exists
        if not os.path.isfile(self.input_file):
            raise ValueError("The input file is missing. Please, introduce as input the file \'core.psimitab\'")


        #########################################################################
        #### NOTE: We are adding only the interactions that have reported methods
        #### The interactions without method are excluded.
        #########################################################################


        # # Add MatrixDB as a valid external entity attribute since it is not recognized by BIANA
        # self.biana_access.add_valid_external_entity_attribute_type( name = "MatrixDB",
        #                                                             data_type = "varchar(20)",
        #                                                             category = "eE identifier attribute")

        # # Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
        # self.biana_access.refresh_database_information()

        # Parse the database
        parser = MatrixDB(self.input_file)
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



        print("\nPARSING OF MATRIXDB FINISHED. THANK YOU FOR YOUR PATIENCE\n")

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

        # Annotate the MatrixDB identifier
        parser.check_id(interactor, 'matrixdb')
        type_id, value_id = parser.analyze_identifier(interactor)
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "MatrixDB", value=value_id, type="unique") )

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

            elif type_id == 'matrixdb':
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

        # Add the MatrixDB identifier of the interaction
        type_id, value_id = parser.analyze_identifier(interaction)
        if type_id.lower() == 'matrixdb' or type_id.lower() == 'matrixdb allergy':
            new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "MatrixDB", value = value_id, type = "unique") )
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






class MatrixDB(object):

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
        self.interaction_types_taxid = {}
        self.interaction_types_compound = {}
        self.interactor_types = {}
        self.same_species = 0
        self.different_species = 0

        return



    def parse(self):

        print("\n.....PARSING THE MATRIXDB INTERACTOME.....\n")

        with open(self.input_file,'r') as input_file_fd:

            first_line = input_file_fd.readline()

            for line in input_file_fd:

                # Split the line in fields
                fields = line.strip().split("\t")

                #0.ID(s) interactor A 
                #1.ID(s) interactor B  
                #2.Alt. ID(s) interactor A 
                #3.Alt. ID(s) interactor B 
                #4.Alias(es) interactor A  
                #5.Alias(es) interactor B  
                #6.Interaction detection method(s) 
                #7.Publication 1st author(s)   
                #8.Publication Identifier(s)   
                #9.Taxid interactor A  
                #10.Taxid interactor B  
                #11.Interaction type(s) 
                #12.Source database(s)  
                #13.Interaction identifier(s)   
                #14.Confidence value(s) 
                #15.Expansion method(s) 
                #16.Biological role(s) interactor A 
                #17.Biological role(s) interactor B 
                #18.Experimental role(s) interactor A   
                #19.Experimental role(s) interactor B   
                #20.Type(s) interactor A    
                #21.Type(s) interactor B    
                #22.Xref(s) interactor A    
                #23.Xref(s) interactor B    
                #24.Interaction Xref(s) 
                #25.Annotation(s) interactor A  
                #26.Annotation(s) interactor B  
                #27.Interaction annotation(s)   
                #28.Host organism(s)    
                #29.Interaction parameter(s)    
                #30.Creation date   
                #31.Update date 
                #32.Checksum(s) interactor A    
                #33.Checksum(s) interactor B    
                #34.Interaction Checksum(s) Negative    
                #35.Feature(s) interactor A 
                #36.Feature(s) interactor B 
                #37.Stoichiometry(s) interactor A   
                #38.Stoichiometry(s) interactor B   
                #39.Identification method participant A 
                #40.Identification method participant B

                # chebi:"CHEBI:28304"   uniprotkb:P55159    matrixdb:GAG_1  -   matrixdb:Heparin(short label)   uniprotkb:"Pon1"(gene name) psi-mi:"MI:0400"("affinity technology") Ori A et al.(2011)  pubmed:21454685|imex:IM-25784   -   taxid:10116(Rattus norvegicus)  psi-mi:"MI:0914"(association)   psi-mi:"MI:0917"(matrixdb)  matrixdb:A0JPN2__B2GUY2__...__Q9Z2H4_21454685_matrixdb_1|intact:EBI-15184828|imex:IM-25784-1    -   psi-mi:"MI:1060"(spoke expansion)   psi-mi:"MI:0499"(unspecified role)  psi-mi:"MI:0499"(unspecified role)  psi-mi:"MI:0496"(bait)  psi-mi:"MI:0498"(prey)  -   -   -   -   -   -   -   figure legend:"Supplemental Table 1"|curation depth:imex curation   taxid:-1(in vitro)  -   -   -   -   -   -   false   -   -   -   -   psi-mi:"MI:0102"(sequence tag identification)   psi-mi:"MI:0102"(sequence tag identification)


                #### Obtain IDENTIFIERS ####

                # Obtain the fields of interest
                identifier1 = fields[0] 
                identifier2 = fields[1] 
                alt_ident1 = fields[2] 
                alt_ident2 = fields[3] 
                aliases1 = fields[4].split('|')
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
                compound1 = False
                compound2 = False

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
                if taxid1 == '-':
                    print('{} is a compound'.format(identifier1))
                    compound1 = True
                else:
                    self.check_id(taxid1, 'taxid')
                    taxid1 = int(taxid1[len('taxid:'):].split('(')[0]) # Get only the taxonomy ID: taxid:9606(Human) --> 9606
                if taxid2 == '-':
                    print('{} is a compound'.format(identifier2))
                    compound2 = True
                else:
                    self.check_id(taxid2, 'taxid')
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
                        method = method[len('psi-mi:'):].split('(')[0] # psi-mi:"MI:0400"("affinity technology") --> "MI:0400"
                        if method.startswith('"MI:'):
                            method = int(method[len('"MI:'):].split('"')[0]) # "MI:0400" --> 400
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
                    #pubmed:21454685|imex:IM-25784
                    all_pubmeds = all_pubmeds.split('|')
                    for pubmed in all_pubmeds:
                        # Collect only the publications coming from pubmed
                        if pubmed.startswith('pubmed:'):
                            pubmed = int(pubmed[len('pubmed:'):]) # pubmed:10359581 --> 10359581
                            pubmeds.append(pubmed)


                #### Obtain SOURCE TYPE OF DATABASES ####
                if all_sources == '-' or all_sources == '':
                    all_sources = '-'
                    print('No source for ids {} {}'.format(identifier1, identifier2))
                else:
                    #psi-mi:"MI:0917"(matrixdb)
                    sources = []
                    all_sources = all_sources.split('|')
                    for source in all_sources:
                        self.check_id(source, 'psi-mi')
                        source = source[len('psi-mi:'):].split('(')[1] # psi-mi:"MI:0917"(matrixdb) --> matrixdb)
                        source = source.split(')')[0] # --> matrixdb
                        sources.append(source)
                        if source.lower() != 'matrixdb':
                            print('Source unknown {} in interaction {}'.format(source, interaction_id))
                            sys.exit(10)


                #### Obtain TYPE OF INTERACTION ####
                if interaction_type == '-' or interaction_type == '':
                    all_sources = '-'
                    print('No interactor type for interaction {}'.format(interaction_id))
                    sys.exit(10)
                else:
                    #psi-mi:"MI:0914"(association)
                    self.check_id(interaction_type, 'psi-mi')
                    interaction_type = interaction_type.split('(')[1] # psi-mi:"MI:0914"(association) --> association)
                    interaction_type = interaction_type.split(')')[0]


                #### Obtain TYPE OF INTERACTOR ####
                if interactor_type1 == '-' or interactor_type1 == '':
                    #print('No type for interactor {}'.format(identifier1))
                    #sys.exit(10)
                    pass
                else:
                    self.check_id(interactor_type1, 'psi-mi')
                    interactor_type1 = interactor_type1.split('(')[1] # psi-mi:"MI:0319"(deoxyribonucleic acid) -->deoxyribonucleic acid)
                    interactor_type1 = interactor_type1.split(')')[0]
                if interactor_type2 == '-' or interactor_type2 == '':
                    #print('No type for interactor {}'.format(identifier2))
                    #sys.exit(10)
                    pass
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
                if alt_ident1 != '-':
                    self.interactor2altids.setdefault(identifier1, set()).add(alt_ident1)
                if alt_ident2 != '-':
                    self.interactor2altids.setdefault(identifier2, set()).add(alt_ident2)
                for alias in aliases1:
                    if alias != '-':
                        self.interactor2altids.setdefault(identifier1, set()).add(alias)
                for alias in aliases2:
                    if alias != '-':
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

                # Insert the type of interaction by taxid
                interaction_type_taxid = frozenset([taxid1, taxid2])
                self.interaction_types_taxid.setdefault(interaction_type_taxid, 0)
                self.interaction_types_taxid[interaction_type_taxid]+=1

                # Insert the type of interaction by compound/protein
                if compound1 and compound2:
                    interaction_type_compound = 'compound-compound'
                else:
                    if compound1 or compound2:
                        interaction_type_compound = 'protein-compound'
                    else:
                        interaction_type_compound = 'protein-protein'
                        if taxid1==taxid2:
                            self.same_species+=1
                        else:
                            self.different_species+=1
                self.interaction_types_compound.setdefault(interaction_type_compound, 0)
                self.interaction_types_compound[interaction_type_compound]+=1

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
                if identifier1 in self.interactor2altids:
                    for alias in self.interactor2altids[identifier1]:
                        self.type_ids.add(self.analyze_identifier(alias)[0])
                if identifier2 in self.interactor2altids:
                    for alias in self.interactor2altids[identifier2]:
                        self.type_ids.add(self.analyze_identifier(alias)[0])


        print(self.type_ids)
        #set(['complex portal', 'uniprotkb', 'chebi', 'matrixdb'])
        print(self.interaction_types)
        #'direct interaction': 1043
        #'physical association': 263
        #'association': 197
        #'protein cleavage': 37
        #'enzymatic reaction': 32
        #'colocalization': 22
        #'cleavage reaction': 9
        #'self interaction': 2
        #'oxidoreductase activity electron transfer reaction': 1
        #'transglutamination reaction': 1
        print(self.interaction_types_taxid)
        # 9606, 9606: 718
        # 10090, 9606: 123
        # 9913, 9606: 75
        # 10090, 10090: 74
        # 10116, 9606: 23
        # 9913, 10090: 10
        # 9913, 9913: 9
        # 9606, 9031: 8
        # 10116, 10116: 6
        # 10144, 9606: 5
        # 9606, 9823: 3
        # 44689, 44689: 3
        # 9606, 11686: 2
        # 9913, 10116: 2
        # 10090, 10116: 2
        # 9913, 9031: 1
        # 9913, 9823: 1
        # 9940, 9606: 1
        # 10144, 9913: 1
        # 9986, 9606: 1
        # 11696, 9606: 1
        print('Interactions between proteins of same species: {}'.format(self.same_species)) #810
        print('Interactions between proteins of different species: {}'.format(self.different_species)) #259

        print(self.interaction_types_compound)
        # 'protein-protein': 1069
        # 'protein-compound': 536
        # 'compound-compound': 2
        sys.exit(10)

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
        fields = identifier.split(':')
        if len(fields) == 2:
            type_identifier, value_identifier = fields
        elif len(fields) > 2:
            type_identifier, value_identifier = fields[0:2]
        else:
            print(identifier)
            print(fields)
            print('Identifier without type at the beginning!')
            sys.exit(10)

        return type_identifier, value_identifier



if __name__ == "__main__":
    main()