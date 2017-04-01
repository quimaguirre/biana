from bianaParser import *
                    
class GPCRParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from the GPCR Interactome (Sokolina et al, Molecular Systems Biology, 2017)

    """                 
                                                                                         
    name = "gpcr"
    description = "This file implements a program that fills up tables in BIANA database from data in the GPCR Interactome"
    external_entity_definition = "An external entity represents a protein"
    external_entity_relations = "An external relation represents a interaction"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "GPCR interactome (Sokolina et al, 2017) parser",  
                             default_script_name = "gpcrParser.py",
                             default_script_description = GPCRParser.description,     
                             additional_compulsory_arguments = [])
                    
    def parse_database(self):
        """                                                                              
        Method that implements the specific operations of a MyData formatted file
        """

        # Check that the input file exists
        if not os.path.isfile(self.input_file):
            raise ValueError("The input file is missing. Please, download the file \'IID_Sokolina_MSB_2017.txt\'")


        # Parse the database
        parser = GPCR_Interactome(self.input_file)
        parser.parse()
        species = set()
        methods = set()
        for interaction in parser.interaction2species:
            species.add(parser.interaction2species[interaction])
        for interaction in parser.interaction2method:
            methods.add(parser.interaction2method[interaction])
        print(species)
        print(methods)
        sys.exit(10)

        # Defining the dict in which we will store the created external entities
        self.external_entity_ids_dict = {}


        print("\n.....INSERTING THE PROTEINS IN THE DATABASE.....\n")

        for uniprot in parser.uniprots:

            # Create an external entity corresponding to the geneID in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(uniprot):

                #print("Adding protein %s" %(uniprot))
                self.create_protein_external_entity(parser, uniprot) 


        print("\n.....INSERTING THE INTERACTIONS IN THE DATABASE.....\n")

        for interaction in parser.interaction2uniprot.keys():

            #print("Adding interaction: {}".format(interaction))
            self.create_gene_disease_association_entity(parser, interaction)


        print("\n.....INSERTING THE SNP-DISEASE ASSOCIATIONS IN THE DATABASE.....\n")

        for SDassociation in parser.SDassociation2snpID.keys():

            #print("Adding SNP-disease association: {}".format(SDassociation))
            self.create_SNP_disease_association_entity(parser, SDassociation)


        print("\nPARSING OF DisGeNET FINISHED. THANK YOU FOR YOUR PATIENCE\n")

        return



    def create_protein_external_entity(self, parser, uniprot):
        """
        Create an external entity of a protein and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        # Annotate its GeneID
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotAccession", value=uniprot, type="cross-reference") )

        # Associate its GeneSymbol
        if uniprot in parser.uniprot2genesymbol:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneSymbol", value=parser.uniprot2genesymbol[uniprot].upper(), type="cross-reference") )
        else:
            print("Gene Symbol not available for %s" %(uniprot))
            pass

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[geneID] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_interaction_entity(self, parser, interaction):
        """
        Create an external entity relation of an interaction and add it in BIANA
        """

        # CREATE THE EXTERNAL ENTITY RELATION
        # Create an external entity relation corresponding to a protein-protein interaction
        new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "interaction" )

        # Add the proteins in the association
        for uniprot in parser.interaction2uniprot[interaction]:
            new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[uniprot] )

        self.interaction2uniprot = {}
        self.interaction2species = {}
        self.interaction2evidence = {}
        self.interaction2method = {}

        # Add the species of the interaction
        if species in parser.interaction2species[interaction]:
            if species == 'human':
                taxID = 9606
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "taxID",
                                                                                                                 value = taxID, type = "cross-reference" ) )
            else:
                print("THE INTERACTION IS NOT HUMAN!!")
                sys.exit(10)
        else:
            print("Species not available for %s" %(interaction))
            pass

        # Add the DisGeNET source of the association
        if GDassociation in parser.GDassociation2source:
            for source in parser.GDassociation2source[GDassociation]:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "DisGeNET_source",
                                                                                                                 value = source, type = "unique" ) )
        else:
            print("DisGeNET source not available for %s" %(GDassociation))
            pass

        # Insert this external entity relation into database
        self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return


    def create_SNP_disease_association_entity(self, parser, SDassociation):
        """
        Create an external entity relation of a SNP-disease association and add it in BIANA
        """

        # CREATE THE EXTERNAL ENTITY RELATION
        # Create an external entity relation corresponding to association between SNP and disease in database
        new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "SNP_disease_association" )

        # Add the gene in the association
        snpID = parser.SDassociation2snpID[SDassociation]
        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[snpID] )

        # Add the disease in the association
        disease_UMLS = parser.SDassociation2disease[SDassociation]
        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[disease_UMLS] )

        # Add the DisGeNET score of the association
        if SDassociation in parser.SDassociation2score:
            new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "DisGeNET_score",
                                                                                                             value = parser.SDassociation2score[SDassociation], type = "unique" ) )
        else:
            print("DisGeNET score not available for %s" %(SDassociation))
            pass

        # Add the DisGeNET source of the association
        if SDassociation in parser.SDassociation2source:
            for source in parser.SDassociation2source[SDassociation]:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "DisGeNET_source",
                                                                                                                 value = source, type = "unique" ) )
        else:
            print("DisGeNET source not available for %s" %(SDassociation))
            pass

        # Add the PubMed id of the association
        if SDassociation in parser.SDassociation2pubmedID:
            new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "Pubmed",
                                                                                                             value = parser.SDassociation2pubmedID[SDassociation], type = "cross-reference" ) )
        else:
            print("Pubmed not available for %s" %(SDassociation))
            pass

        # Add the sentence of the association
        if SDassociation in parser.SDassociation2sentence:
            new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "Description",
                                                                                                             value = parser.SDassociation2sentence[SDassociation] ) )
        else:
            print("Sentence not available for %s" %(SDassociation))
            pass

        # Insert this external entity relation into database
        self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return




class GPCR_Interactome(object):

    def __init__(self, input_file):

        self.GPCR_file = input_file

        self.uniprots = set()
        self.uniprot2genesymbol = {}

        self.interaction2uniprot = {}
        self.interaction2species = {}
        self.interaction2evidence = {}
        self.interaction2method = {}

        return



    def parse(self):

        #################################
        #### PARSE GENE-DISEASE FILE ####
        #################################

        print("\n.....PARSING GENE-DISEASE ASSOCIATIONS FILE.....\n")

        gpcr_file_fd = open(self.GPCR_file,'r')

        first_line = gpcr_file_fd.readline()

        # Obtain a dictionary: "field_name" => "position"
        fields_dict = self.obtain_header_fields(first_line)
        #UniProt 1   UniProt 2   Symbol 1    Symbol 2    Species Evidence Type   Detection Method

        for line in gpcr_file_fd:

            # Split the line in fields
            fields = line.strip().split("\t")

            # Obtain the fields of interest
            uniprot1 = fields[ fields_dict['UniProt 1'] ]
            uniprot2 = fields[ fields_dict['UniProt 2'] ]
            symbol1 = fields[ fields_dict['Symbol 1'] ]
            symbol2 = fields[ fields_dict['Symbol 2'] ]
            specie = fields[ fields_dict['Species'] ]
            evidence_type = fields[ fields_dict['Evidence Type'] ]
            detection_method = fields[ fields_dict['Detection Method'] ]

            # Create an interaction id for the protein-protein interaction
            # ---> interaction id = uniprot1 + '---' + uniprot2
            interaction = uniprot1 + '---' + uniprot2

            # Insert the fields into dictionaries
            self.uniprots.add(uniprot1)
            self.uniprot2genesymbol[uniprot1] = symbol1
            self.uniprots.add(uniprot2)
            self.uniprot2genesymbol[uniprot2] = symbol2

            self.interaction2uniprot.setdefault(interaction, [])
            self.interaction2uniprot[interaction].append(uniprot1)
            self.interaction2uniprot[interaction].append(uniprot2)
            self.interaction2species[interaction] = specie
            self.interaction2evidence[interaction] = evidence_type
            self.interaction2method[interaction] = detection_method

        gpcr_file_fd.close()

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



if __name__ == "__main__":
    main()