from bianaParser import *
import csv

class humanproteinatlasParser(BianaParser):                                                        
    """             
    humanproteinatlas Parser Class 

    Parses data from the Human Protein Atlas database

    """                 
                                                                                         
    name = "humanproteinatlas"
    description = "This file implements a program that fills up tables in BIANA database from data of the Human Protein Atlas database"
    external_entity_definition = "An external entity represents a gene"
    external_entity_relations = "No external entity relations"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "The Human Protein Atlas parser",
                             default_script_name = "humanproteinatlasParser.py",
                             default_script_description = humanproteinatlasParser.description,
                             additional_compulsory_arguments = [])
        self.default_eE_attribute = "ensembl"
                    
    def parse_database(self):
        """     
        Method that implements the specific operations of a MyData formatted file
        """

        #### Check that all the files exist ####

        if os.path.isdir(self.input_file):
            self.input_path = self.input_file
        else:
            raise ValueError("You must specify a path instead of a file")

        files_list = [f for f in os.listdir(self.input_path) if os.path.isfile(os.path.join(self.input_path, f))]

        files = ["normal_tissue.csv", "rna_tissue.csv"]

        for current_file in files:
            if current_file not in files_list:
                raise ValueError("File %s is missing in %s" %(current_file, self.input_path))


        #### Add new external entity attributes in BIANA ####

        # Add a different type of external entity
        self.biana_access.add_valid_external_entity_type( type = "tissue" )

        # Add a different type of relation
        self.biana_access.add_valid_external_entity_relation_type( type = "gene_tissue_association" )

        # Add different type of external entity attributes
        self.biana_access.add_valid_external_entity_attribute_type( name = "HumanProteinAtlas_tissue",
                                                                        data_type = "varchar(100)",
                                                                        category = "eE identifier attribute")
        self.biana_access.add_valid_external_entity_attribute_type( name = "HumanProteinAtlas_celltype",
                                                                        data_type = "varchar(100)",
                                                                        category = "eE identifier attribute")
        self.biana_access.add_valid_external_entity_attribute_type( name = "HumanProteinAtlas_level",
                                                                        data_type = 'ENUM("high", "medium", "low", "not detected")',
                                                                        category = "eE identifier attribute")
        self.biana_access.add_valid_external_entity_attribute_type( name = "HumanProteinAtlas_reliability",
                                                                        data_type = 'ENUM("supported", "approved", "uncertain")',
                                                                        category = "eE identifier attribute")

        data_type_dict = {"fields": [ ("value","double"),
                                     ("unit","varchar(15)", False)
                                   ],
                          "indices": ("value",)} # Stores a regex

        # Add different type of external entity attribute
        self.biana_access.add_valid_external_entity_attribute_type( name = "HumanProteinAtlas_RNAseq_value",
                                                                        data_type = data_type_dict,
                                                                        category = "eE special attribute")



        # Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
        self.biana_access.refresh_database_information()


        #### Reliability score explanation ####

        # A reliability score is manually set for all genes and indicates the level of reliability of the analyzed protein expression pattern based on available RNA-seq data and/or protein/gene characterization data.
        # The reliability score is divided into Supported, Approved, or Uncertain. 
        # Supported is indicated by a star on the image that links to the tissue atlas data for a particular gene. 
        # If there is available data from more than one antibody, the staining patterns of all antibodies are taken in consideration during evaluation of reliability score.
        
        # Supported - Consistency with RNA-seq and/or protein/gene characterization data, in combination with similar staining pattern if independent antibodies are available.
        # Approved - Consistency with RNA-seq data in combination with inconsistency with, or lack of, protein/gene characterization data. Alternatively, consistency with protein/gene characterization data in combination with inconsistency with RNA-seq data. If independent antibodies are available, the staining pattern is partly similar or dissimilar.
        # Uncertain - Inconsistency with, or lack of, RNA-seq and/or protein/gene characterization data, in combination with dissimilar staining pattern if independent antibodies are available.

        # Source: http://www.proteinatlas.org/about/assays+annotation#ihcr


        #### Parse the database ####

        parser = HumanProteinAtlas(self.input_path)
        parser.parse_HPA()


        
        # Defining the dict in which we will store the created external entities
        self.external_entity_ids_dict = {}



        print("\n.....ADDING THE GENE EXTERNAL ENTITIES.....\n")

        for gene in parser.genes:

            if not self.external_entity_ids_dict.has_key(gene):
                #print("Adding gene %s" %(gene))
                self.create_gene_external_entity(parser, gene)



        print("\n.....ADDING THE TISSUE-CELL EXTERNAL ENTITIES.....\n")

        # Create the external entities of tissues
        for tissuecell in parser.tissuecells:

            if not self.external_entity_ids_dict.has_key(tissuecell):
                #print("Adding tissue %s" %(BTO))
                self.create_tissuecell_external_entity(parser, tissuecell)



        print("\n.....ADDING THE TISSUE EXTERNAL ENTITIES.....\n")

        # Create the external entities of tissues
        for tissue in parser.tissues:

            if not self.external_entity_ids_dict.has_key(tissue):
                #print("Adding tissue %s" %(BTO))
                self.create_tissue_external_entity(parser, tissue)



        print("\n.....ADDING THE GENE-TISSUE MICROARRAY ASSOCIATIONS.....\n")

        # Create the associations
        for gene in parser.gene2tissuecell2evidence:

            for tissuecell in parser.gene2tissuecell2evidence[gene]:

                self.create_gene_tissue_association_entity(parser, gene, tissuecell)


        print("\n.....ADDING THE GENE-TISSUE RNA-SEQ ASSOCIATIONS.....\n")

        # Create the associations
        for gene in parser.gene2tissue2values:

            for tissue in parser.gene2tissue2values[gene]:

                self.create_gene_tissue_rnaseq_association_entity(parser, gene, tissue)


        return



    def create_gene_external_entity(self, parser, gene):
        """
        Create an external entity of a gene and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        # Annotate its gene identifier
        if gene.startswith('ENS'):
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Ensembl", value=gene, type="unique") )
        else:
            print('Identifier Unknown for gene: {}'.format(gene))
            sys.exit(10)
            #new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneSymbol", value=gene, type="unique") )

        # Associate its Taxonomy ID
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "TaxID", value=9606, type="cross-reference") )

        # Associate its gene symbol
        if gene in parser.gene2genesymbol:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneSymbol", value=parser.gene2genesymbol[gene].upper(), type="unique") )
        else:
            print("No gene symbol for {}".format(gene))
            sys.exit(10)

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[gene] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_tissuecell_external_entity(self, parser, tissuecell):
        """
        Create an external entity of a cell type inside a tissue and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "tissue" )

        tissue = parser.tissuecell2values[tissuecell]['tissue']
        cell_type = parser.tissuecell2values[tissuecell]['cell_type']

        # Annotate the tissue name
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "HumanProteinAtlas_tissue", value=tissue, type="unique") )

        # Annotate its cell types
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "HumanProteinAtlas_celltype", value=cell_type, type="unique") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[tissuecell] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_tissue_external_entity(self, parser, tissue):
        """
        Create an external entity of a tissue and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "tissue" )

        # Annotate the tissue name
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "HumanProteinAtlas_tissue", value=tissue, type="unique") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[tissue] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_gene_tissue_association_entity(self, parser, gene, tissuecell):
        """
        Create an external entity relation of a gene-tissue association and add it in BIANA
        """

        # CREATE THE EXTERNAL ENTITY RELATION
        # Create an external entity relation corresponding to association between gene and disease in database
        new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "gene_tissue_association" )

        # Add the gene in the association
        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[gene] )

        # Add the tissue + cell_type in the association
        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[tissuecell] )

        level = parser.gene2tissuecell2evidence[gene][tissuecell]['level']
        reliability = parser.gene2tissuecell2evidence[gene][tissuecell]['reliability']

        # Add the additional attributes of the association
        new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "HumanProteinAtlas_level",
                                                                                                         value = level, type = "unique" ) )
        new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "HumanProteinAtlas_reliability",
                                                                                                         value = reliability, type = "unique" ) )

        # Insert this external entity relation into database
        self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return


    def create_gene_tissue_rnaseq_association_entity(self, parser, gene, tissue):
        """
        Create an external entity relation of a gene-tissue association from RNAseq data and add it in BIANA
        """

        # CREATE THE EXTERNAL ENTITY RELATION
        # Create an external entity relation corresponding to association between gene and disease in database
        new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "gene_tissue_association" )

        # Add the gene in the association
        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[gene] )

        # Add the tissue + cell_type in the association
        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[tissue] )

        value = parser.gene2tissue2values[gene][tissue]['value']
        unit = parser.gene2tissue2values[gene][tissue]['unit']

        # Add the additional attributes of the association
        new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "HumanProteinAtlas_RNAseq_value", value=value, type="unique",
                                                                                         additional_fields = {"unit": unit} ) )

        # Insert this external entity relation into database
        self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return



class HumanProteinAtlas(object):

    def __init__(self, input_path):

        # For normal_tissue file
        self.tissue_file = os.path.join(input_path, "normal_tissue.csv")

        self.tissuecells = set()
        self.tissuecell2values = {}

        self.genes = set()
        self.gene2genesymbol = {}
        self.gene2tissuecell2evidence = {}

        # For rna_tissue file
        self.rnaseq_file = os.path.join(input_path, "rna_tissue.csv")

        self.tissues = set()
        self.gene2tissue2values = {}

        return


    def parse_HPA(self):

        print("\n.....PARSING THE NORMAL TISSUE FILE.....\n")

        normal_tissue_fd = open(self.tissue_file,'r')

        normal_tissue_fd.readline()

        data = csv.reader(normal_tissue_fd, delimiter=',')

        #"Gene","Gene name","Tissue","Cell type","Level","Reliability"
        #"ENSG00000000003","TSPAN6","adrenal gland","glandular cells","Not detected","Approved"
        for fields in data:
            (ensembl, genesymbol, tissue, cell_type, level, reliability) = fields

            tissue = tissue.lower()
            cell_type = cell_type.lower()

            self.genes.add(ensembl)
            self.gene2genesymbol[ensembl] = genesymbol

            # Create an id for the tissue and cell type:
            tissuecell = '{}---{}'.format(tissue, cell_type)

            self.tissuecells.add(tissuecell)
            self.tissuecell2values.setdefault(tissuecell, {})
            self.tissuecell2values[tissuecell]['tissue'] = tissue
            self.tissuecell2values[tissuecell]['cell_type'] = cell_type

            self.gene2tissuecell2evidence.setdefault(ensembl, {})
            self.gene2tissuecell2evidence[ensembl].setdefault(tissuecell, {})
            self.gene2tissuecell2evidence[ensembl][tissuecell]['level'] = level.lower()
            self.gene2tissuecell2evidence[ensembl][tissuecell]['reliability'] = reliability.lower()

        normal_tissue_fd.close()


        print("\n.....PARSING THE RNASEQ FILE.....\n")

        rnaseq_fd = open(self.rnaseq_file,'r')

        rnaseq_fd.readline()

        data = csv.reader(rnaseq_fd, delimiter=',')

        #"Gene","Gene name","Sample","Value","Unit"
        #"ENSG00000000003","TSPAN6","adipose tissue","31.5","TPM"
        for fields in data:
            (ensembl, genesymbol, tissue, value, unit) = fields

            tissue = tissue.lower()

            self.genes.add(ensembl)
            self.gene2genesymbol[ensembl] = genesymbol

            self.tissues.add(tissue)

            self.gene2tissue2values.setdefault(ensembl, {})
            self.gene2tissue2values[ensembl].setdefault(tissue, {})
            self.gene2tissue2values[ensembl][tissue]['value'] = float(value)
            self.gene2tissue2values[ensembl][tissue]['unit'] = unit.upper()

        rnaseq_fd.close()


        return


if __name__ == "__main__":
    main()