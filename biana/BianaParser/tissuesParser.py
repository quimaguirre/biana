from bianaParser import *
                    
class tissuesParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from the Tissues database (Jensen Lab)

    """                 
                                                                                         
    name = "tissues"
    description = "This file implements a program that fills up tables in BIANA database from data of the database Tissues (Jensen Lab)"
    external_entity_definition = "An external entity represents a gene"
    external_entity_relations = "No external entity relations"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "Tissues parser",
                             default_script_name = "tissuesParser.py",
                             default_script_description = tissuesParser.description,
                             additional_compulsory_arguments = [])
                    
    def parse_database(self):
        """     
        Method that implements the specific operations of a MyData formatted file
        """

        # Check that all the files exist
        if os.path.isdir(self.input_file):
            self.input_path = self.input_file
        else:
            raise ValueError("You must specify a path instead of a file")

        # Species name to Taxonomy ID
        self.species2taxid = {
            'human' : 9606,
            'mouse' : 10090,
            'rat'   : 10116,
            'pig'   : 9823
        }

        # data_type_dict = {"fields": [ ("value","varchar(50)"),
        #                              ("BTO_name","varchar(100)", False),
        #                              ("source","varchar(50)", False),
        #                              ("evidence","varchar(50)", False),
        #                              ("confidence","varchar(50)", False)
        #                            ],
        #                   "indices": ("value",)} # Stores a regex


        # # Add different type of external entity attribute
        # self.biana_access.add_valid_external_entity_attribute_type( name = "Tissues_BTO",
        #                                                                 data_type = data_type_dict,
        #                                                                 category = "eE special attribute")

        # Add a different type of external entity
        self.biana_access.add_valid_external_entity_type( type = "tissue" )

        # Add a different type of relation
        self.biana_access.add_valid_external_entity_relation_type( type = "gene_tissue_association" )

        # Add different type of external entity attributes
        self.biana_access.add_valid_external_entity_attribute_type( name = "BTO",
                                                                        data_type = "integer(7) unsigned",
                                                                        category = "eE attribute")
        self.biana_access.add_valid_external_entity_attribute_type( name = "BTO_name",
                                                                        data_type = "varchar(370)",
                                                                        category = "eE identifier attribute")
        self.biana_access.add_valid_external_entity_attribute_type( name = "TissuesSource",
                                                                        data_type = "varchar(30)",
                                                                        category = "eE identifier attribute")
        self.biana_access.add_valid_external_entity_attribute_type( name = "TissuesEvidence",
                                                                        data_type = "varchar(30)",
                                                                        category = "eE identifier attribute")
        self.biana_access.add_valid_external_entity_attribute_type( name = "TissuesConfidence",
                                                                        data_type = "varchar(30)",
                                                                        category = "eE identifier attribute")

        # Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
        self.biana_access.refresh_database_information()



        # Obtain all the files inside the input folder
        files_list = [os.path.join(self.input_path, f) for f in os.listdir(self.input_path) if os.path.isfile(os.path.join(self.input_path, f))]


        # Define the Tissues class
        parser = Tissues()

        # Process the file names and parse the files
        for current_file in files_list:

            basename = os.path.basename(current_file)
            (species, _, type_file, _) = basename.split('_')
            if species in self.species2taxid:
                taxID = self.species2taxid[species]
            else:
                print('The species {} is not in the dictionary species2taxid.\nPlease, search the Taxonomy ID of the species in www.uniprot.org/taxonomy and add it in the dictionary'.format(species))
                sys.exit(10)        

            parser.parse(current_file, type_file, taxID)


        # Defining the dict in which we will store the created external entities
        self.external_entity_ids_dict = {}


        print("\n.....ADDING THE GENE EXTERNAL ENTITIES.....\n")

        # Create the external entities of genes
        for genetax in parser.genetax_ids:

            if not self.external_entity_ids_dict.has_key(genetax):
                #print("Adding gene %s" %(genetax))
                self.create_gene_external_entity(parser, genetax)

        print("\n.....ADDING THE TISSUE EXTERNAL ENTITIES.....\n")

        # Create the external entities of tissues
        for BTO in parser.BTOs:

            if not self.external_entity_ids_dict.has_key(BTO):
                #print("Adding tissue %s" %(BTO))
                self.create_tissue_external_entity(parser, BTO)

        print("\n.....ADDING THE GENE-TISSUE ASSOCIATIONS.....\n")

        # Create the associations
        for genetax in parser.genetax_ids:

            if genetax in parser.genetax2BTO:

                for BTO in parser.genetax2BTO[genetax]:

                    self.create_gene_tissue_association_entity(parser, genetax, BTO)

            else:
                print("No BTO for {}".format(genetax))
                sys.exit(10)


        return



    def create_gene_external_entity(self, parser, genetax):
        """
        Create an external entity of a gene and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        gene = parser.genetax2gene[genetax]
        taxID = parser.genetax2tax[genetax]

        # Annotate its gene identifier
        if gene.startswith('ENS'):
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Ensembl", value=gene, type="unique") )
        else:
            gene = gene.split(' {ECO:')[0]
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneSymbol", value=gene, type="unique") )

        # Associate its Taxonomy ID
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "TaxID", value=taxID, type="cross-reference") )

        # Associate its synonym
        if genetax in parser.genetax2synonym:
            for synonym in parser.genetax2synonym[genetax]:
                if synonym.startswith('ENS'):
                    new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Ensembl", value=synonym.upper(), type="synonym") )
                else:
                    synonym = synonym.split(' {ECO:')[0]
                    new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneSymbol", value=synonym.upper(), type="synonym") )
        else:
            print("No synonym for gene: {}".format(genetax))
            sys.exit(10)

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[genetax] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_tissue_external_entity(self, parser, BTO):
        """
        Create an external entity of a tissue and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "tissue" )

        # Annotate its BTO term
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "BTO", value=BTO, type="unique") )

        # # Annotate its BTO name
        # new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "BTO_name", value=parser.BTO2name[BTO], type="unique") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[BTO] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_gene_tissue_association_entity(self, parser, genetax, BTO):
        """
        Create an external entity relation of a gene-tissue association and add it in BIANA
        """

        for type_file in parser.genetax2BTO[genetax][BTO]:

            for x in xrange(len(parser.genetax2BTO[genetax][BTO][type_file]['source'])):

                source = parser.genetax2BTO[genetax][BTO][type_file]['source'][x]
                confidence = parser.genetax2BTO[genetax][BTO][type_file]['confidence'][x]

                if 'evidence' in parser.genetax2BTO[genetax][BTO][type_file]:
                    evidence = parser.genetax2BTO[genetax][BTO][type_file]['evidence'][x]
                else:
                    evidence = "text-mining"


                # CREATE THE EXTERNAL ENTITY RELATION
                # Create an external entity relation corresponding to association between gene and disease in database
                new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "gene_tissue_association" )


                # Add the gene in the association
                new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[genetax] )

                # Add the tissue in the association
                new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[BTO] )

                # Add the additional attributes of the association
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "TissuesSource",
                                                                                                                 value = source, type = "unique" ) )
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "TissuesEvidence",
                                                                                                                 value = evidence, type = "unique" ) )
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "TissuesConfidence",
                                                                                                                 value = confidence, type = "unique" ) )

                # Insert this external entity relation into database
                self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return



class Tissues(object):

    def __init__(self):

        self.genetax_ids = set()
        self.BTOs = set()

        self.genetax2gene = {}
        self.genetax2tax = {}
        self.genetax2synonym = {}
        self.genetax2BTO = {}
        self.BTO2name = {}

        return


    def parse(self, input_file, type_file, taxID):

        f = open(input_file, 'r')

        print("\n.....PARSING THE FILE: {}.....\n".format(os.path.basename(input_file)))


        if type_file == 'knowledge' or type_file == 'experiments':

            for line in f:
                # Split the line in fields
                # Snord14d  Snord14d    BTO:0001042 Amygdala    GNF V3  16955 expression units  2
                (gene, synonym, BTO_term, BTO_name, source, evidence, confidence) = line.strip().split("\t")

                # Skip the cases of 0 confidence
                if float(confidence) <= 0:
                    continue

                # Obtain the BTO number
                temp = re.search("BTO\:(\d+)",BTO_term)
                if temp:
                    BTO = temp.group(1)
                else:
                    continue # If it is not in Brenda Tissue Ontology, we skip it

                genetax = '{}---{}'.format(gene, taxID)
                self.genetax_ids.add(genetax)

                self.genetax2gene[genetax] = gene
                self.genetax2tax[genetax] = taxID

                self.genetax2synonym.setdefault(genetax, set())
                self.genetax2synonym[genetax].add(synonym)

                self.BTOs.add(BTO)
                self.BTO2name[BTO] = BTO_name

                self.genetax2BTO.setdefault(genetax, {})
                self.genetax2BTO[genetax].setdefault(BTO, {})
                self.genetax2BTO[genetax][BTO].setdefault(type_file, {})
                self.genetax2BTO[genetax][BTO][type_file].setdefault('source', [])
                self.genetax2BTO[genetax][BTO][type_file].setdefault('evidence', [])
                self.genetax2BTO[genetax][BTO][type_file].setdefault('confidence', [])
                self.genetax2BTO[genetax][BTO][type_file]['source'].append(source)
                self.genetax2BTO[genetax][BTO][type_file]['evidence'].append(evidence)
                self.genetax2BTO[genetax][BTO][type_file]['confidence'].append(confidence)


            f.close()

        elif type_file == 'textmining':

            for line in f:
                # Split the line in fields
                # Snord14d  Snord14d    BTO:0001042 Amygdala    GNF V3  16955 expression units  2
                (gene, synonym, BTO_term, BTO_name, score, confidence, url) = line.strip().split("\t")

                # Skip the cases of 0 confidence
                if float(confidence) <= 0:
                    continue

                # Obtain the BTO number
                temp = re.search("BTO\:(\d+)",BTO_term)
                if temp:
                    BTO = temp.group(1)
                else:
                    continue # If it is not in Brenda Tissue Ontology, we skip it

                genetax = '{}---{}'.format(gene, taxID)
                self.genetax_ids.add(genetax)

                self.genetax2gene[genetax] = gene
                self.genetax2tax[genetax] = taxID

                self.genetax2synonym.setdefault(genetax, set())
                self.genetax2synonym[genetax].add(synonym)

                self.BTOs.add(BTO)
                self.BTO2name[BTO] = BTO_name

                self.genetax2BTO.setdefault(genetax, {})
                self.genetax2BTO[genetax].setdefault(BTO, {})
                self.genetax2BTO[genetax][BTO].setdefault(type_file, {})
                self.genetax2BTO[genetax][BTO][type_file].setdefault('source', [])
                self.genetax2BTO[genetax][BTO][type_file].setdefault('confidence', [])
                self.genetax2BTO[genetax][BTO][type_file]['source'].append('text-mining')
                self.genetax2BTO[genetax][BTO][type_file]['confidence'].append(confidence)
                self.BTO2name[BTO] = BTO_name

            f.close()


        return



if __name__ == "__main__":
    main()