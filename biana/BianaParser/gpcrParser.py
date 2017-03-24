from bianaParser import *
                    
class GPCRParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from the GPCR Interactome (Sokolina et al, Molecular Systems Biology, 2017)

    """                 
                                                                                         
    name = "GPCR"
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


        # Defining the dict in which we will store the created external entities
        self.external_entity_ids_dict = {}


        print("\n.....INSERTING THE GENES IN THE DATABASE.....\n")

        for geneID in parser.geneIDs:

            # Create an external entity corresponding to the geneID in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(geneID):

                #print("Adding gene %s" %(geneID))
                self.create_gene_external_entity(parser, geneID)


        print("\n.....INSERTING THE DISEASES IN THE DATABASE.....\n")

        for disease_UMLS in parser.diseaseUMLS:

            # Create an external entity corresponding to the disease in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(disease_UMLS):

                #print("Adding disease %s" %(disease_UMLS))
                self.create_disease_external_entity(parser, disease_UMLS)


        print("\n.....INSERTING THE SNPs IN THE DATABASE.....\n")

        for snpID in parser.snpIDs:

            # Create an external entity corresponding to the SNP in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(snpID):

                #print("Adding SNP %s" %(snpID))
                self.create_SNP_external_entity(parser, snpID)


        print("\n.....INSERTING THE GENE-DISEASE ASSOCIATIONS IN THE DATABASE.....\n")

        for GDassociation in parser.GDassociation2geneID.keys():

            #print("Adding gene-disease association: {}".format(GDassociation))
            self.create_gene_disease_association_entity(parser, GDassociation)


        print("\n.....INSERTING THE SNP-DISEASE ASSOCIATIONS IN THE DATABASE.....\n")

        for SDassociation in parser.SDassociation2snpID.keys():

            #print("Adding SNP-disease association: {}".format(SDassociation))
            self.create_SNP_disease_association_entity(parser, SDassociation)


        print("\nPARSING OF DisGeNET FINISHED. THANK YOU FOR YOUR PATIENCE\n")

        return



    def create_gene_external_entity(self, parser, geneID):
        """
        Create an external entity of a gene and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "gene" )

        # Annotate its GeneID
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneID", value=geneID, type="cross-reference") )

        # Associate its GeneSymbol
        if geneID in parser.geneID2name:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneSymbol", value=parser.geneID2name[geneID].upper(), type="cross-reference") )
        else:
            print("Name not available for %s" %(geneID))
            pass

        # Associate its description
        if geneID in parser.geneID2description:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Description", value=parser.geneID2description[geneID] ) )
        else:
            print("Description not available for %s" %(geneID))
            pass

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[geneID] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_disease_external_entity(self, parser, disease_UMLS):
        """
        Create an external entity of a disease and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "disease" )

        # Annotate its disease_UMLS
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UMLS_diseaseID", value=disease_UMLS, type="cross-reference") )

        # Associate its name
        if disease_UMLS in parser.disease2name:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Name", value=parser.disease2name[disease_UMLS], type="cross-reference") )
        else:
            print("Name not available for %s" %(disease_UMLS))
            pass

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[disease_UMLS] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_SNP_external_entity(self, parser, snpID):
        """
        Create an external entity of a SNP and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "SNP" )

        # Annotate its snpID
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "SNP", value=snpID, type="cross-reference") )

        # Associate the GeneID of its gene
        if snpID in parser.snpID2geneID:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneID", value=parser.snpID2geneID[snpID], type="cross-reference") )
        else:
            print("GeneID not available for %s" %(snpID))
            pass

        # Associate the GeneSymbol of its gene
        if snpID in parser.snpID2geneSymbol:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneSymbol", value=parser.snpID2geneSymbol[snpID].upper(), type="cross-reference") )
        else:
            print("GeneSymbol not available for %s" %(snpID))
            pass

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[snpID] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_gene_disease_association_entity(self, parser, GDassociation):
        """
        Create an external entity relation of a gene-disease association and add it in BIANA
        """

        # CREATE THE EXTERNAL ENTITY RELATION
        # Create an external entity relation corresponding to association between gene and disease in database
        new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "gene_disease_association" )

        # Add the gene in the association
        geneID = parser.GDassociation2geneID[GDassociation]
        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[geneID] )

        # Add the disease in the association
        disease_UMLS = parser.GDassociation2disease[GDassociation]
        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[disease_UMLS] )

        # Add the DisGeNET score of the association
        if GDassociation in parser.GDassociation2score:
            new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "DisGeNET_score",
                                                                                                             value = parser.GDassociation2score[GDassociation], type = "unique" ) )
        else:
            print("DisGeNET score not available for %s" %(GDassociation))
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
        self.snp_file = path + '/all_snps_sentences_pubmeds_position.tsv'

        self.geneIDs = set()
        self.geneID2name = {}
        self.geneID2description = {}

        self.diseaseUMLS = set()
        self.disease2name = {}

        self.GDassociation2disease = {}
        self.GDassociation2geneID = {}
        self.GDassociation2score = {}
        self.GDassociation2source = {}

        self.snpIDs = set()
        self.snpID2geneID = {}
        self.snpID2geneSymbol = {}

        self.SDassociation2disease = {}
        self.SDassociation2snpID = {}
        self.SDassociation2score = {}
        self.SDassociation2source = {}
        self.SDassociation2pubmedID = {}
        self.SDassociation2sentence = {}

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

            # Create an association id for the gene-disease association
            # ---> association id = geneID + '---' + disease_UMLS
            association = geneID + '---' + disease_UMLS

            # Insert the fields into dictionaries
            self.geneIDs.add(geneID)
            if geneName != 'NA':
                self.geneID2name[geneID] = geneName
            if description != 'NA':
                self.geneID2description[geneID] = description

            self.diseaseUMLS.add(disease_UMLS)
            if diseaseName != 'NA':
                self.disease2name[disease_UMLS] = diseaseName
            
            self.GDassociation2disease[association] = disease_UMLS
            self.GDassociation2geneID[association] = geneID
            self.GDassociation2score[association] = score
            self.GDassociation2source[association] = source

        gene_disease_file_fd.close()



        ########################
        #### PARSE SNP FILE ####
        ########################

        print("\n.....PARSING GENE-DISEASE ASSOCIATIONS FILE.....\n")

        snp_file_fd = open(self.snp_file,'r')

        num_line = 0

        for line in snp_file_fd:

            # Skip comments
            if line[0] == '#':
                continue

            num_line += 1

            # Obtain a dictionary: "field_name" => "position"
            if num_line == 1:
                fields_dict = self.obtain_header_fields(line)
                continue

            # Split the line in fields
            fields = line.strip().split("\t")

            # Obtain the fields of interest
            snpID = fields[ fields_dict['snpId'] ]
            pubmedID = fields[ fields_dict['pubmedId'] ]
            geneID = fields[ fields_dict['geneId'] ]
            geneSymbol = fields[ fields_dict['geneSymbol'] ]
            disease_UMLS = fields[ fields_dict['diseaseId'] ]
            diseaseName = fields[ fields_dict['diseaseName'] ]
            source = fields[ fields_dict['sourceId'] ].split(',')
            sentence = fields[ fields_dict['sentence'] ]
            score = fields[ fields_dict['score'] ]
            year = fields[ fields_dict['year'] ]

            # Create an association id for the SNP-disease association
            # ---> association id = snpID + '---' + disease_UMLS
            association = snpID + '---' + disease_UMLS

            # Insert the fields into dictionaries
            self.snpIDs.add(snpID)
            if geneID != 'NA':
                self.snpID2geneID[snpID] = geneID
            if geneSymbol != 'NA':
                self.snpID2geneSymbol[snpID] = geneSymbol

            self.diseaseUMLS.add(disease_UMLS)
            if diseaseName != 'NA':
                self.disease2name[disease_UMLS] = diseaseName

            self.SDassociation2disease[association] = disease_UMLS
            self.SDassociation2snpID[association] = snpID
            self.SDassociation2score[association] = score
            self.SDassociation2source[association] = source
            if pubmedID != 'NA':
                self.SDassociation2pubmedID[association] = pubmedID
            if sentence != 'NA':
                self.SDassociation2sentence[association] = sentence

        snp_file_fd.close()

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