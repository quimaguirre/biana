from bianaParser import *
import gzip
                    
class DisGeNETParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from DisGeNET

    """                 
                                                                                         
    name = "disgenet"
    description = "This file implements a program that fills up tables in BIANA database from data in DisGeNET"
    external_entity_definition = "An external entity represents a gene, SNP or disease"
    external_entity_relations = "An external relation represents a gene-disease association, or SNP-disease association"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "DisGeNET parser",  
                             default_script_name = "DisGeNETParser.py",
                             default_script_description = DisGeNETParser.description,     
                             additional_compulsory_arguments = [])
        self.default_eE_attribute = "geneid"
                    
    def parse_database(self):
        """                                                                              
        Method that implements the specific operations of a MyData formatted file
        """

        #####################################
        #### DEFINE NEW BIANA CATEGORIES ####
        #####################################

        # Add a different type of external entity
        self.biana_access.add_valid_external_entity_type( type = "SNP" )
        self.biana_access.add_valid_external_entity_type( type = "disease" )

        # Add a different type of relation
        self.biana_access.add_valid_external_entity_relation_type( type = "gene_disease_association" )
        self.biana_access.add_valid_external_entity_relation_type( type = "SNP_disease_association" )
        self.biana_access.add_valid_external_entity_relation_type( type = "gene_SNP_association" )

        # Add different type of external entity attributes

        self.biana_access.add_valid_external_entity_attribute_type( name = "UMLS_CUI",
                                                                        data_type = "varchar(30)",
                                                                        category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "dbSNP",
                                                                        data_type = "varchar(30)",
                                                                        category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "DisGeNET_score",
                                                                        data_type = "double",
                                                                        category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "DisGeNET_source",
                                                                        data_type = "varchar(30)",
                                                                        category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "DisGeNET_type_association",
                                                                        data_type = "varchar(30)",
                                                                        category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "DisGeNET_disease_type",
                                                                        data_type = "varchar(30)",
                                                                        category = "eE identifier attribute")

        # Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
        self.biana_access.refresh_database_information()



        ###########################
        #### CHECK INPUT FILES ####
        ###########################

        # Check that the input path exists
        if os.path.isdir(self.input_file):
            self.input_path = self.input_file
        else:
            raise ValueError("You must specify a path instead of a file")

        # Check that the necessary files exist
        self.gene_disease_file = self.input_path + '/all_gene_disease_pmid_associations.tsv.gz'
        self.snp_disease_file = self.input_path + '/all_variant_disease_pmid_associations.tsv.gz'
        self.snp_file = self.input_path + '/snp_genes.gz'

        if not os.path.isfile(self.gene_disease_file):
            print('The file \'all_gene_disease_pmid_associations.tsv\' is not in the input path!')
            sys.exit(10)

        if not os.path.isfile(self.snp_disease_file):
            print('The file \'all_variant_disease_pmid_associations.tsv\' is not in the input path!')
            sys.exit(10)

        if not os.path.isfile(self.snp_file):
            print('The file \'snp_genes.gz\' is not in the input path! Download it at \'ftp://ftp.ncbi.nih.gov/snp/Entrez/eLinks/snp_genes.gz\'')
            sys.exit(10)



        ########################
        #### PARSE DATABASE ####
        ########################

        # Parse the database
        parser = DisGeNET(self.input_path)
        parser.parse()



        #########################################
        #### INSERT PARSED DATABASE IN BIANA ####
        #########################################

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


        print("\n.....INSERTING THE GENE-SNP ASSOCIATIONS IN THE DATABASE.....\n")

        for GSassociation in parser.GSassociation2geneID.keys():

            geneID = parser.GSassociation2geneID[GSassociation]
            snpID = parser.GSassociation2snpID[GSassociation]

            if not self.external_entity_ids_dict.has_key(geneID):
                self.create_gene_external_entity(parser, geneID)

            if not self.external_entity_ids_dict.has_key(snpID):
                self.create_SNP_external_entity(parser, snpID)

            self.create_gene_snp_association_entity(geneID, snpID)


        print("\nPARSING OF DisGeNET FINISHED. THANK YOU FOR YOUR PATIENCE\n")

        return



    def create_gene_external_entity(self, parser, geneID):
        """
        Create an external entity of a gene and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        # Annotate its GeneID
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneID", value=geneID, type="cross-reference") )

        # Associate its GeneSymbol
        if geneID in parser.geneID2genesymbol:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneSymbol", value=parser.geneID2genesymbol[geneID].upper(), type="cross-reference") )
        else:
            #print("Name not available for %s" %(geneID))
            pass

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[geneID] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_disease_external_entity(self, parser, disease_UMLS):
        """
        Create an external entity of a disease and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "disease" )

        # Annotate its disease UMLS CUI
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UMLS_CUI", value=disease_UMLS, type="cross-reference") )

        # Annotate the type of disease
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DisGeNET_disease_type", value=parser.disease2type[disease_UMLS], type="unique") )

        # Associate its name
        if disease_UMLS in parser.disease2name:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Name", value=parser.disease2name[disease_UMLS].lower(), type="unique") )
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
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "dbSNP", value=snpID, type="unique") )

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

        # Add the PubMed ids of the association
        if GDassociation in parser.GDassociation2pubmedID:
            for pmid in parser.GDassociation2pubmedID[GDassociation]:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "Pubmed",
                                                                                                                 value = pmid, type = "unique" ) )
        else:
            #print("Pubmed not available for %s" %(GDassociation))
            pass

        # Add the type of associations
        if GDassociation in parser.GDassociation2type:
            for type_assoc in parser.GDassociation2type[GDassociation]:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "DisGeNET_type_association",
                                                                                                                 value = type_assoc, type = "unique" ) )
        else:
            print("Type of association not available for %s" %(GDassociation))
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
            for pmid in parser.SDassociation2pubmedID[SDassociation]:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "Pubmed",
                                                                                                                 value = pmid, type = "unique" ) )
        else:
            #print("Pubmed not available for %s" %(SDassociation))
            pass

        # # Add the sentence of the association
        # if SDassociation in parser.SDassociation2sentence:
        #     new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "Description",
        #                                                                                                      value = parser.SDassociation2sentence[SDassociation] ) )
        # else:
        #     print("Sentence not available for %s" %(SDassociation))
        #     pass

        # Insert this external entity relation into database
        self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return


    def create_gene_snp_association_entity(self, geneID, snpID):
        """
        Create an external entity relation of a gene-SNP association and add it in BIANA
        """

        # CREATE THE EXTERNAL ENTITY RELATION
        # Create an external entity relation corresponding to association between gene and SNP in database
        new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "gene_SNP_association" )

        # Add the gene in the association
        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[geneID] )

        # Add the SNP in the association
        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[snpID] )

        # Insert this external entity relation into database
        self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return




class DisGeNET(object):

    def __init__(self, path):

        self.gene_disease_file = path + '/all_gene_disease_pmid_associations.tsv.gz'
        self.snp_disease_file = path + '/all_variant_disease_pmid_associations.tsv.gz'
        self.snp_file = path + '/snp_genes.gz'

        self.geneIDs = set()
        self.geneID2genesymbol = {}

        self.diseaseUMLS = set()
        self.disease2name = {}
        self.disease2type = {}

        self.GDassociation2disease = {}
        self.GDassociation2geneID = {}
        self.GDassociation2score = {}
        self.GDassociation2source = {}
        self.GDassociation2type = {}
        self.GDassociation2pubmedID = {}

        self.snpIDs = set()

        self.SDassociation2disease = {}
        self.SDassociation2snpID = {}
        self.SDassociation2score = {}
        self.SDassociation2source = {}
        self.SDassociation2pubmedID = {}
        #self.SDassociation2sentence = {}

        self.GSassociation2geneID = {}
        self.GSassociation2snpID = {}

        return



    def parse(self):

        #################################
        #### PARSE GENE-DISEASE FILE ####
        #################################

        print("\n.....PARSING GENE-DISEASE ASSOCIATIONS FILE.....\n")

        gene_disease_file_fd = gzip.open(self.gene_disease_file, 'rb')

        num_line = 0

        for line in gene_disease_file_fd:

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
            # geneId    diseaseId   associationType sentence    pmid    score   originalSource  diseaseName diseaseType geneSymbol
            geneID = fields[ fields_dict['geneId'] ]
            geneSymbol = fields[ fields_dict['geneSymbol'] ]
            disease_UMLS = fields[ fields_dict['diseaseId'] ]
            diseaseName = fields[ fields_dict['diseaseName'] ]
            diseaseType = fields[ fields_dict['diseaseType'] ]
            score = float(fields[ fields_dict['score'] ])
            source = fields[ fields_dict['originalSource'] ]
            associationType = fields[ fields_dict['associationType'] ]
            pubmedID = fields[ fields_dict['pmid'] ]

            # Create an association id for the gene-disease association
            # ---> association id = geneID + '---' + disease_UMLS
            association = geneID + '---' + disease_UMLS

            # Insert the fields into dictionaries
            self.geneIDs.add(geneID)
            if geneSymbol != 'NA':
                self.geneID2genesymbol[geneID] = geneSymbol

            self.diseaseUMLS.add(disease_UMLS)
            if diseaseName != 'NA':
                self.disease2name[disease_UMLS] = diseaseName
            if diseaseType != 'NA':
                self.disease2type[disease_UMLS] = diseaseType

            self.GDassociation2disease[association] = disease_UMLS
            self.GDassociation2geneID[association] = geneID
            self.GDassociation2score[association] = score

            self.GDassociation2source.setdefault(association, set())
            self.GDassociation2source[association].add(source)

            self.GDassociation2type.setdefault(association, set())
            self.GDassociation2type[association].add(associationType)

            if pubmedID != 'NA':
                self.GDassociation2pubmedID.setdefault(association, set())
                self.GDassociation2pubmedID[association].add(pubmedID)

        gene_disease_file_fd.close()



        ################################
        #### PARSE SNP-DISEASE FILE ####
        ################################

        print("\n.....PARSING SNP-DISEASE ASSOCIATIONS FILE.....\n")

        snp_disease_file_fd = gzip.open(self.snp_disease_file, 'rb')

        num_line = 0

        for line in snp_disease_file_fd:

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
            # snpId diseaseId   sentence    pmid    score   originalSource  diseaseName diseaseType chromosome  position
            snpID = fields[ fields_dict['snpId'] ]
            pubmedID = fields[ fields_dict['pmid'] ]
            #geneID = fields[ fields_dict['geneId'] ]
            #geneSymbol = fields[ fields_dict['geneSymbol'] ]
            disease_UMLS = fields[ fields_dict['diseaseId'] ]
            diseaseName = fields[ fields_dict['diseaseName'] ]
            diseaseType = fields[ fields_dict['diseaseType'] ]
            score = float(fields[ fields_dict['score'] ])
            source = fields[ fields_dict['originalSource'] ]
            #sentence = fields[ fields_dict['sentence'] ]
            #year = fields[ fields_dict['year'] ]

            # Create an association id for the SNP-disease association
            # ---> association id = snpID + '---' + disease_UMLS
            association = snpID + '---' + disease_UMLS

            # Insert the fields into dictionaries
            self.snpIDs.add(snpID)

            # if geneID != 'NA':
            #     self.snpID2geneID[snpID] = geneID
            # if geneSymbol != 'NA':
            #     self.snpID2geneSymbol[snpID] = geneSymbol

            self.diseaseUMLS.add(disease_UMLS)
            if diseaseName != 'NA':
                self.disease2name[disease_UMLS] = diseaseName
            if diseaseType != 'NA':
                self.disease2type[disease_UMLS] = diseaseType

            self.SDassociation2disease[association] = disease_UMLS
            self.SDassociation2snpID[association] = snpID
            self.SDassociation2score[association] = score
            self.SDassociation2source.setdefault(association, set())
            self.SDassociation2source[association].add(source)
            if pubmedID != 'NA':
                self.SDassociation2pubmedID.setdefault(association, set())
                self.SDassociation2pubmedID[association].add(pubmedID)
            # if sentence != 'NA':
            #     self.SDassociation2sentence[association] = sentence

        snp_disease_file_fd.close()



        #############################
        #### PARSE SNP-GENE FILE ####
        #############################

        print("\n.....PARSING SNP-GENE ASSOCIATIONS FILE.....\n")

        snp_file_fd = gzip.open(self.snp_file, 'rb')

        for line in snp_file_fd:

            # Split the line in fields
            fields = line.strip().split("\t")
            snp_num = fields[0]
            geneID = fields[1]
            snpID = 'rs'+snp_num

            # Skip the SNPs that are not in DisGeNET
            if snpID not in self.snpIDs:
                continue

            association = '{}---{}'.format(snpID, geneID)
            self.GSassociation2geneID[association] = geneID
            self.GSassociation2snpID[association] = snpID


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