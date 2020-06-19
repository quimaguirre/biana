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

        self.biana_access.add_valid_external_entity_attribute_type( name = "DisGeNET_score",
                                                                    data_type = "double",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "DisGeNET_EI",
                                                                    data_type = "double",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "DisGeNET_source",
                                                                    data_type = "varchar(50)",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "DisGeNET_DSI",
                                                                    data_type = "double",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "DisGeNET_DPI",
                                                                    data_type = "double",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "DisGeNET_disease_type",
                                                                    data_type = "varchar(30)",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "dbSNP",
                                                                    data_type = "varchar(30)",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "UMLS_CUI",
                                                                    data_type = "varchar(30)",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "UMLS_name",
                                                                    data_type = "varchar(500)",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "UMLS_semantic_type",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "DOID",
                                                                    data_type = "varchar(30)",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "DO_name",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "MESH_ID",
                                                                    data_type = "varchar(30)",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "MESH_name",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "ICD10_ID",
                                                                    data_type = "varchar(30)",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "ICD10_name",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "ICD10CM_ID",
                                                                    data_type = "varchar(30)",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "ICD10CM_name",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "OMIM_ID",
                                                                    data_type = "varchar(30)",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "OMIM_name",
                                                                    data_type = "varchar(500)",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "HPO_ID",
                                                                    data_type = "varchar(30)",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "HPO_name",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "EFO_ID",
                                                                    data_type = "varchar(30)",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "EFO_name",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "SNP_chromosome",
                                                                    data_type = "varchar(30)",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "SNP_position",
                                                                    data_type = "varchar(30)",
                                                                    category = "eE descriptive attribute")


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
        self.gene_disease_file = os.path.join(self.input_path, 'all_gene_disease_pmid_associations.tsv.gz')
        self.snp_disease_file = os.path.join(self.input_path, 'all_variant_disease_pmid_associations.tsv.gz')
        self.snp_gene_file = os.path.join(self.input_path, 'variant_to_gene_mappings.tsv.gz')
        self.disease_mappings_file = os.path.join(self.input_path, 'disease_mappings.tsv.gz')

        if not os.path.isfile(self.gene_disease_file):
            print('The file \'all_gene_disease_pmid_associations.tsv.gz\' is not in the input path!')
            sys.exit(10)

        if not os.path.isfile(self.snp_disease_file):
            print('The file \'all_variant_disease_pmid_associations.tsv.gz\' is not in the input path!')
            sys.exit(10)

        if not os.path.isfile(self.snp_gene_file):
            print('The file \'variant_to_gene_mappings.tsv.gz\' is not in the input path!')
            sys.exit(10)

        if not os.path.isfile(self.disease_mappings_file):
            print('The file \'disease_mappings.tsv.gz\' is not in the input path!')
            sys.exit(10)



        ########################
        #### PARSE DATABASE ####
        ########################

        # Parse the database
        parser = DisGeNET(self.input_path)
        parser.parse_gene_disease_file()
        parser.parse_snp_disease_file()
        parser.parse_snp_gene_file()
        parser.parse_disease_mappings_file()


        #########################################
        #### INSERT PARSED DATABASE IN BIANA ####
        #########################################

        # Defining the dict in which we will store the created external entities
        self.external_entity_ids_dict = {}


        print("\n.....INSERTING THE GENES IN THE DATABASE.....\n")

        for geneID in parser.geneIDs:

            # Create an external entity corresponding to the geneID in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(geneID):

                #print("Adding gene {}".format(geneID))
                self.create_gene_external_entity(parser, geneID)


        print("\n.....INSERTING THE DISEASES IN THE DATABASE.....\n")

        for disease_UMLS in parser.diseaseUMLS:

            # Create an external entity corresponding to the disease in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(disease_UMLS):

                #print("Adding disease {}".format(disease_UMLS))
                self.create_disease_external_entity(parser, disease_UMLS)


        print("\n.....INSERTING THE SNPs IN THE DATABASE.....\n")

        for snpID in parser.snpIDs:

            # Create an external entity corresponding to the SNP in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(snpID):

                #print("Adding SNP {}".format(snpID))
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

            self.create_gene_snp_association_entity(parser, GSassociation)


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

        # Associate its DSI
        if geneID in parser.gene2dsi:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DisGeNET_DSI", value=parser.gene2dsi[geneID], type="unique") )
        else:
            #print("DSI not available for {}".format(geneID))
            pass

        # Associate its DPI
        if geneID in parser.gene2dpi:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DisGeNET_DPI", value=parser.gene2dpi[geneID], type="unique") )
        else:
            #print("DSI not available for {}".format(geneID))
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
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UMLS_CUI", value=disease_UMLS.upper(), type="cross-reference") )

        # Annotate the type of disease
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DisGeNET_disease_type", value=parser.disease2type[disease_UMLS].lower(), type="unique") )

        # Associate its name
        if disease_UMLS in parser.disease2name:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UMLS_name", value=parser.disease2name[disease_UMLS].lower(), type="cross-reference") )
        else:
            print("Name not available for {}".format(disease_UMLS))
            pass

        # Associate its semantic type
        if disease_UMLS in parser.disease2semantictype:
            for semantic_type in parser.disease2semantictype[disease_UMLS]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UMLS_semantic_type", value=semantic_type.lower(), type="cross-reference") )
        else:
            #print("Semantic type not available for {}".format(disease_UMLS))
            pass

        # Associate its vocabulary mapping
        if disease_UMLS in parser.disease2vocabulary2code2name:
            # Distinct vocabularies: 'ICD10CM', 'DO', 'ICD10', 'EFO', 'ORDO', 'MSH', 'NCI', 'OMIM', 'HPO', 'ICD9CM', 'MONDO'
            for vocabulary in parser.disease2vocabulary2code2name[disease_UMLS]:
                # ADD MESH
                if vocabulary == 'MSH':
                    for code in parser.disease2vocabulary2code2name[disease_UMLS][vocabulary]:
                        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "MESH_ID", value=code.upper(), type="cross-reference") )
                        for vocabulary_name in parser.disease2vocabulary2code2name[disease_UMLS][vocabulary][code]:
                            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "MESH_name", value=vocabulary_name.lower(), type="cross-reference") )
                # ADD DISEASE ONTOLOGY
                if vocabulary == 'DO':
                    for code in parser.disease2vocabulary2code2name[disease_UMLS][vocabulary]:
                        doid_code = 'DOID:'+code
                        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DOID", value=doid_code, type="cross-reference") )
                        for vocabulary_name in parser.disease2vocabulary2code2name[disease_UMLS][vocabulary][code]:
                            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DO_name", value=vocabulary_name.lower(), type="cross-reference") )
                # ADD ICD-10
                if vocabulary == 'ICD10':
                    for code in parser.disease2vocabulary2code2name[disease_UMLS][vocabulary]:
                        if '-' in code and '.' in code:
                            alt_code = code.split('.')[0] # in the cases where there is a range finished with decimal (e.g. E70-E90.9) we the codes both with and without the decimal
                            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "ICD10_ID", value=alt_code.upper(), type="cross-reference") )
                        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "ICD10_ID", value=code.upper(), type="cross-reference") )
                        for vocabulary_name in parser.disease2vocabulary2code2name[disease_UMLS][vocabulary][code]:
                            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "ICD10_name", value=vocabulary_name.lower(), type="cross-reference") )
                # ADD ICD-10-CM
                if vocabulary == 'ICD10CM':
                    for code in parser.disease2vocabulary2code2name[disease_UMLS][vocabulary]:
                        if '-' in code and '.' in code:
                            alt_code = code.split('.')[0] # in the cases where there is a range finished with decimal (e.g. E70-E90.9) we the codes both with and without the decimal
                            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "ICD10CM_ID", value=alt_code.upper(), type="cross-reference") )
                        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "ICD10CM_ID", value=code.upper(), type="cross-reference") )
                        for vocabulary_name in parser.disease2vocabulary2code2name[disease_UMLS][vocabulary][code]:
                            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "ICD10CM_name", value=vocabulary_name.lower(), type="cross-reference") )
                # ADD OMIM
                if vocabulary == 'OMIM':
                    for code in parser.disease2vocabulary2code2name[disease_UMLS][vocabulary]:
                        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "OMIM_ID", value=code, type="cross-reference") )
                        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "MIM", value=code, type="cross-reference") )
                        for vocabulary_name in parser.disease2vocabulary2code2name[disease_UMLS][vocabulary][code]:
                            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "OMIM_name", value=vocabulary_name.lower(), type="cross-reference") )
                # ADD HPO
                if vocabulary == 'HPO':
                    for code in parser.disease2vocabulary2code2name[disease_UMLS][vocabulary]:
                        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "HPO_ID", value=code.upper(), type="cross-reference") )
                        for vocabulary_name in parser.disease2vocabulary2code2name[disease_UMLS][vocabulary][code]:
                            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "HPO_name", value=vocabulary_name.lower(), type="cross-reference") )
                # ADD EFO
                if vocabulary == 'EFO':
                    for code in parser.disease2vocabulary2code2name[disease_UMLS][vocabulary]:
                        efo_code = 'EFO:'+code
                        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "EFO_ID", value=efo_code.upper(), type="cross-reference") )
                        for vocabulary_name in parser.disease2vocabulary2code2name[disease_UMLS][vocabulary][code]:
                            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "EFO_name", value=vocabulary_name.lower(), type="cross-reference") )
        else:
            #print("Vocabulary mapping not available for {}".format(disease_UMLS))
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

        # Associate its chromosome
        if snpID in parser.snp2chromosome:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "SNP_chromosome", value=parser.snp2chromosome[snpID], type="unique") )
        else:
            #print("Chromosome not available for {}".format(snpID))
            pass

        # Associate its position
        if snpID in parser.snp2position:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "SNP_position", value=parser.snp2position[snpID], type="unique") )
        else:
            #print("Chromosome not available for {}".format(snpID))
            pass

        # Associate its DSI
        if snpID in parser.snp2dsi:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DisGeNET_DSI", value=parser.snp2dsi[snpID], type="unique") )
        else:
            #print("DSI not available for {}".format(snpID))
            pass

        # Associate its DPI
        if snpID in parser.snp2dpi:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DisGeNET_DPI", value=parser.snp2dpi[snpID], type="unique") )
        else:
            #print("DSI not available for {}".format(snpID))
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
            print("DisGeNET score not available for {}".format(GDassociation))
            pass

        # Add the DisGeNET Evidence Index (EI) of the association
        if GDassociation in parser.GDassociation2ei:
            new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "DisGeNET_EI",
                                                                                                             value = parser.GDassociation2ei[GDassociation], type = "unique" ) )
        else:
            #print("DisGeNET EI not available for {}".format(GDassociation))
            pass

        # Add the DisGeNET source of the association
        if GDassociation in parser.GDassociation2source:
            for source in parser.GDassociation2source[GDassociation]:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "DisGeNET_source",
                                                                                                                 value = source, type = "unique" ) )
        else:
            print("DisGeNET source not available for {}".format(GDassociation))
            pass

        # Add the PubMed ids of the association
        if GDassociation in parser.GDassociation2pubmedID:
            for pmid in parser.GDassociation2pubmedID[GDassociation]:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "Pubmed",
                                                                                                                 value = pmid, type = "unique" ) )
        else:
            #print("Pubmed not available for {}".format(GDassociation))
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
            print("DisGeNET score not available for {}".format(SDassociation))
            pass

        # Add the DisGeNET Evidence Index (EI) of the association
        if SDassociation in parser.SDassociation2ei:
            new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "DisGeNET_EI",
                                                                                                             value = parser.SDassociation2ei[SDassociation], type = "unique" ) )
        else:
            #print("DisGeNET EI not available for {}".format(SDassociation))
            pass

        # Add the DisGeNET source of the association
        if SDassociation in parser.SDassociation2source:
            for source in parser.SDassociation2source[SDassociation]:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "DisGeNET_source",
                                                                                                                 value = source, type = "unique" ) )
        else:
            print("DisGeNET source not available for {}".format(SDassociation))
            pass

        # Add the PubMed id of the association
        if SDassociation in parser.SDassociation2pubmedID:
            for pmid in parser.SDassociation2pubmedID[SDassociation]:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "Pubmed",
                                                                                                                 value = pmid, type = "unique" ) )
        else:
            #print("Pubmed not available for {}".format(SDassociation))
            pass

        # # Add the sentence of the association
        # if SDassociation in parser.SDassociation2sentence:
        #     new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "Description",
        #                                                                                                      value = parser.SDassociation2sentence[SDassociation] ) )
        # else:
        #     print("Sentence not available for {}".format(SDassociation))
        #     pass

        # Insert this external entity relation into database
        self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return


    def create_gene_snp_association_entity(self, parser, GSassociation):
        """
        Create an external entity relation of a gene-SNP association and add it in BIANA
        """

        # CREATE THE EXTERNAL ENTITY RELATION
        # Create an external entity relation corresponding to association between gene and SNP in database
        new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "gene_SNP_association" )

        # Add the gene in the association
        geneID = parser.GSassociation2geneID[GSassociation]
        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[geneID] )

        # Add the SNP in the association
        snpID = parser.GSassociation2snpID[GSassociation]
        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[snpID] )

        # Add the DisGeNET source of the association
        if GSassociation in parser.GSassociation2source:
            for source in parser.GSassociation2source[GSassociation]:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "DisGeNET_source",
                                                                                                                 value = source, type = "unique" ) )
        else:
            print("DisGeNET source not available for {}".format(GSassociation))
            pass

        # Insert this external entity relation into database
        self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return




class DisGeNET(object):

    def __init__(self, path):

        self.gene_disease_file = os.path.join(path, 'all_gene_disease_pmid_associations.tsv.gz')
        self.snp_disease_file = os.path.join(path, 'all_variant_disease_pmid_associations.tsv.gz')
        self.snp_gene_file = os.path.join(path, 'variant_to_gene_mappings.tsv.gz')
        self.disease_mappings_file = os.path.join(path, 'disease_mappings.tsv.gz')

        self.geneIDs = set()
        self.geneID2genesymbol = {}
        self.gene2dsi = {}
        self.gene2dpi = {}

        self.diseaseUMLS = set()
        self.disease2name = {}
        self.disease2type = {}
        self.disease2class = {}
        self.disease2semantictype = {}
        self.disease2vocabulary2code2name = {}

        self.GDassociation2disease = {}
        self.GDassociation2geneID = {}
        self.GDassociation2score = {}
        self.GDassociation2ei = {}
        self.GDassociation2source = {}
        self.GDassociation2pubmedID = {}

        self.snpIDs = set()
        self.snp2chromosome = {}
        self.snp2position = {}
        self.snp2dsi = {}
        self.snp2dpi = {}

        self.SDassociation2disease = {}
        self.SDassociation2snpID = {}
        self.SDassociation2score = {}
        self.SDassociation2source = {}
        self.SDassociation2pubmedID = {}
        self.SDassociation2ei = {}

        self.GSassociation2geneID = {}
        self.GSassociation2snpID = {}
        self.GSassociation2source = {}

        return



    def parse_gene_disease_file(self):

        #################################
        #### PARSE GENE-DISEASE FILE ####
        #################################

        print("\n.....PARSING GENE-DISEASE-PMID ASSOCIATIONS FILE.....\n")

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
            # geneId	geneSymbol	DSI	DPI	diseaseId	diseaseName	diseaseType	diseaseClass	diseaseSemanticType	score	EI	YearInitial	YearFinal	pmid	source
            geneID = fields[ fields_dict['geneId'] ]
            geneSymbol = fields[ fields_dict['geneSymbol'] ]
            DSI = fields[ fields_dict['DSI'] ] # Disease Specificity Index for the gene
            DPI = fields[ fields_dict['DPI'] ] # Disease Pleiotropy Index for the gene
            disease_UMLS = fields[ fields_dict['diseaseId'] ]
            diseaseName = fields[ fields_dict['diseaseName'] ]
            diseaseType = fields[ fields_dict['diseaseType'] ] # The DisGeNET disease type: disease, phenotype and group
            diseaseClass = fields[ fields_dict['diseaseClass'] ].split(';') # The MeSH disease class(es) (e.g. C18;C10)
            diseaseSemanticType = fields[ fields_dict['diseaseSemanticType'] ].split('; ') # The UMLS Semantic Type(s) of the disease (e.g. Disease or Syndrome; Congenital Abnormality)
            score = float(fields[ fields_dict['score'] ]) # DisGENET score for the Gene-Disease association
            EI = fields[ fields_dict['EI'] ] # Evidence Index (EI), which indicates the existence of contradictory results in the publications supporting the associations. An EI equal to one indicates that all the publications support the GDA or the VDA, while an EI smaller than one indicates that there are publications that assert that there is no association between the gene/variants and the disease
            pubmedID = fields[ fields_dict['pmid'] ]
            source = fields[ fields_dict['source'] ]
            #print(geneID, disease_UMLS)

            # Create an association id for the gene-disease association
            # ---> association id = geneID + '---' + disease_UMLS
            association = geneID + '---' + disease_UMLS

            # Insert the fields into dictionaries
            # Some fields are missing in some associations! For example:
            # 2	A2M	0.529	0.769	C0085400	Neurofibrillary degeneration (morphologic abnormality)	phenotype		Cell or Molecular Dysfunction	0.1					HPO
            # 3	A2MP1			C0010200	Coughing	phenotype	C23;C08	Sign or Symptom	0.01	1	2017	2017	28073367	BEFREE
            self.geneIDs.add(geneID)
            if geneSymbol != 'NA' and geneSymbol != '':
                self.geneID2genesymbol[geneID] = geneSymbol
            if DSI != 'NA' and DSI != '':
                self.gene2dsi[geneID] = float(DSI)
            if DPI != 'NA' and DPI != '':
                self.gene2dpi[geneID] = float(DPI)

            self.diseaseUMLS.add(disease_UMLS)
            if diseaseName != 'NA' and diseaseName != '':
                self.disease2name[disease_UMLS] = diseaseName
            if diseaseType != 'NA' and diseaseType != '':
                self.disease2type[disease_UMLS] = diseaseType
            if diseaseClass != 'NA' and diseaseClass != '':
                for dclass in diseaseClass:
                    self.disease2class.setdefault(disease_UMLS, set()).add(dclass)
            if diseaseSemanticType != 'NA' and diseaseSemanticType != '':
                for dstype in diseaseSemanticType:
                    self.disease2semantictype.setdefault(disease_UMLS, set()).add(dstype)

            self.GDassociation2disease[association] = disease_UMLS
            self.GDassociation2geneID[association] = geneID
            self.GDassociation2score[association] = score

            if EI != 'NA' and EI != '':
                self.GDassociation2ei[association] = float(EI)

            if pubmedID != 'NA' and pubmedID != '':
                self.GDassociation2pubmedID.setdefault(association, set()).add(pubmedID)

            self.GDassociation2source.setdefault(association, set()).add(source)


        gene_disease_file_fd.close()

        return



    def parse_snp_disease_file(self):

        ################################
        #### PARSE SNP-DISEASE FILE ####
        ################################

        print("\n.....PARSING SNP-DISEASE-PMID ASSOCIATIONS FILE.....\n")

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
            # snpId	chromosome	position	DSI	DPI	diseaseId	diseaseName	diseaseType	diseaseClass	diseaseSemanticType	score	EI	YearInitial	YearFinal	pmid	source
            snpID = fields[ fields_dict['snpId'] ]
            chromosome = fields[ fields_dict['chromosome'] ]
            position = fields[ fields_dict['position'] ]
            DSI = fields[ fields_dict['DSI'] ] # Disease Specificity Index for the variant
            DPI = fields[ fields_dict['DPI'] ] # Disease Pleiotropy Index for the variant
            disease_UMLS = fields[ fields_dict['diseaseId'] ]
            diseaseName = fields[ fields_dict['diseaseName'] ]
            diseaseType = fields[ fields_dict['diseaseType'] ]
            diseaseClass = fields[ fields_dict['diseaseClass'] ].split(';') # The MeSH disease class(es) (e.g. C18;C10)
            diseaseSemanticType = fields[ fields_dict['diseaseSemanticType'] ].split('; ') # The UMLS Semantic Type(s) of the disease (e.g. Disease or Syndrome; Congenital Abnormality)
            score = float(fields[ fields_dict['score'] ])
            EI = fields[ fields_dict['EI'] ] # The Evidence Index for the Variant-Disease association
            pubmedID = fields[ fields_dict['pmid'] ]
            source = fields[ fields_dict['source'] ]

            # Create an association id for the SNP-disease association
            # ---> association id = snpID + '---' + disease_UMLS
            association = snpID + '---' + disease_UMLS

            # Insert the fields into dictionaries
            self.snpIDs.add(snpID)
            if chromosome != 'NA' and chromosome != '':
                self.snp2chromosome[snpID] = chromosome
            if position != 'NA' and position != '':
                self.snp2position[snpID] = int(position)
            if DSI != 'NA' and DSI != '':
                self.snp2dsi[snpID] = float(DSI)
            if DPI != 'NA' and DPI != '':
                self.snp2dpi[snpID] = float(DPI)

            self.diseaseUMLS.add(disease_UMLS)
            if diseaseName != 'NA' and diseaseName != '':
                self.disease2name[disease_UMLS] = diseaseName
            if diseaseType != 'NA' and diseaseType != '':
                self.disease2type[disease_UMLS] = diseaseType
            if diseaseClass != 'NA' and diseaseClass != '':
                for dclass in diseaseClass:
                    self.disease2class.setdefault(disease_UMLS, set()).add(dclass)
            if diseaseSemanticType != 'NA' and diseaseSemanticType != '':
                for dstype in diseaseSemanticType:
                    self.disease2semantictype.setdefault(disease_UMLS, set()).add(dstype)

            self.SDassociation2disease[association] = disease_UMLS
            self.SDassociation2snpID[association] = snpID
            self.SDassociation2score[association] = score

            if EI != 'NA' and EI != '':
                self.SDassociation2ei[association] = float(EI)

            if pubmedID != 'NA' and pubmedID != '':
                self.SDassociation2pubmedID.setdefault(association, set()).add(pubmedID)

            self.SDassociation2source.setdefault(association, set()).add(source)

        snp_disease_file_fd.close()

        return



    def parse_snp_gene_file(self):

        #############################
        #### PARSE SNP-GENE FILE ####
        #############################

        print("\n.....PARSING SNP-GENE ASSOCIATIONS FILE.....\n")

        snp_gene_file_fd = gzip.open(self.snp_gene_file, 'rb')

        for line in snp_gene_file_fd:

            # Split the line in fields
            # snpId	geneId	geneSymbol	sourceId
            fields = line.strip().split("\t")
            snpID = fields[0]
            geneID = fields[1]
            geneSymbol = fields[2]
            sourceID = fields[3]

            self.geneIDs.add(geneID)
            if geneSymbol != 'NA' and geneSymbol != '':
                self.geneID2genesymbol[geneID] = geneSymbol

            association = '{}---{}'.format(snpID, geneID)
            self.GSassociation2geneID[association] = geneID
            self.GSassociation2snpID[association] = snpID
            self.GSassociation2source[association] = sourceID

        snp_gene_file_fd.close()

        return



    def parse_disease_mappings_file(self):

        #####################################
        #### PARSE DISEASE MAPPINGS FILE ####
        #####################################

        print("\n.....PARSING DISEASE MAPPINGS FILE.....\n")

        disease_mappings_file_fd = gzip.open(self.disease_mappings_file, 'rb')

        num_line = 0

        for line in disease_mappings_file_fd:

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
            # diseaseId	name	vocabulary	code	vocabularyName
            disease_UMLS = fields[ fields_dict['diseaseId'] ]
            diseaseName = fields[ fields_dict['name'] ]
            vocabulary = fields[ fields_dict['vocabulary'] ] # Vocabulary short name
            code = fields[ fields_dict['code'] ] # Vocabulary code of the disease
            vocabularyName = fields[ fields_dict['vocabularyName'] ] # Vocabulary disease name

            self.diseaseUMLS.add(disease_UMLS)
            if diseaseName != 'NA' and diseaseName != '':
                self.disease2name[disease_UMLS] = diseaseName
            # Distinct vocabularies: set(['ICD10CM', 'DO', 'ICD10', 'EFO', 'ORDO', 'MSH', 'NCI', 'OMIM', 'HPO', 'ICD9CM', 'MONDO'])
            self.disease2vocabulary2code2name.setdefault(disease_UMLS, {})
            self.disease2vocabulary2code2name[disease_UMLS].setdefault(vocabulary, {})
            self.disease2vocabulary2code2name[disease_UMLS][vocabulary].setdefault(code, set()).add(vocabularyName)

        disease_mappings_file_fd.close()

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