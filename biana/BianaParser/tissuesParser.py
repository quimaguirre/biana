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

        #### PROCESS THE FILE NAMES ####

        # Obtain all the files inside the input folder
        files_list = [os.path.join(self.input_path, f) for f in os.listdir(self.input_path) if os.path.isfile(os.path.join(self.input_path, f))]

        for current_file in files_list:

            basename = os.path.basename(current_file)
            (species, _, type_file, _) = basename.split('_')
            if species in self.species2taxid:
                taxID = self.species2taxid[species]
            else:
                print('The species {} is not in the dictionary species2taxid.\nPlease, search the Taxonomy ID of the species in www.uniprot.org/taxonomy and add it in the dictionary'.format(species))
                sys.exit(10)

        
            # Defining the dict in which we will store the created external entities
            self.external_entity_ids_dict = {}

            parser = Tissues()
            parser.parse(current_file, type_file)

            for gene in parser.genes:

                if not self.external_entity_ids_dict.has_key(gene):
                    #print("Adding gene %s" %(gene))
                    self.create_gene_external_entity(parser, gene, taxID)


        return



    def create_gene_external_entity(self, parser, gene, taxID):
        """
        Create an external entity of a gene and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "gene" )

        # Annotate its gene identifier
        if gene.startswith('ENS'):
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Ensembl", value=gene, type="unique") )
        else:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneSymbol", value=gene, type="unique") )

        # Associate its Taxonomy ID
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "TaxID", value=taxID, type="cross-reference") )

        # Associate its synonym
        if gene in parser.gene2synonym:
            if gene.startswith('ENS'):
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Ensembl", value=parser.gene2synonym[gene].upper(), type="synonym") )
            else:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneSymbol", value=parser.gene2synonym[gene].upper(), type="synonym") )
        else:
            print("No synonym for {}".format(gene))
            sys.exit(10)

        # Associate its BRENDA Tissue Ontology term (BTO term)
        if gene in parser.gene2BTO:
            for BTO in parser.gene2BTO[gene]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "BTO_term", value=BTO.upper(), type="unique") )
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "BTO_name", value=parser.BTO2name[BTO], type="unique") )
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "TissuesSource", value=parser.gene2BTO[gene][BTO]['source'], type="unique") )
                if 'evidence' in parser.gene2BTO[gene][BTO]:
                    new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "TissuesEvidence", value=parser.gene2BTO[gene][BTO]['evidence'], type="unique") )
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "TissuesConfidence", value=parser.gene2BTO[gene][BTO]['confidence'], type="unique") )
        else:
            print("No BTO for {}".format(gene))
            sys.exit(10)

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[gene] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return



class Tissues(object):

    def __init__(self):

        self.genes = set()
        self.gene2synonym = {}
        self.gene2BTO = {}
        self.BTO2name = {}

        return


    def parse(self, input_file, type_file):

        f = open(input_file, 'r')

        print("\n.....PARSING THE FILE: {}.....\n".format(os.path.basename(input_file)))

        if type_file == 'knowledge' or type_file == 'experiments':

            for line in f:
                # Split the line in fields
                # Snord14d  Snord14d    BTO:0001042 Amygdala    GNF V3  16955 expression units  2
                (gene, synonym, BTO, BTO_name, source, evidence, confidence) = line.strip().split("\t")

                self.genes.add(gene)

                if gene not in self.gene2synonym:
                    self.gene2synonym[gene] = synonym
                else:
                    if self.gene2synonym[gene] != synonym:
                        print('Gene {} with two different synonyms: {} and {}'.format(gene, synonym, self.gene2synonym[gene]))
                        sys.exit(10)

                self.gene2BTO.setdefault(gene, {})
                self.gene2BTO[gene].setdefault(BTO, {})
                self.gene2BTO[gene][BTO]['source'] = source
                self.gene2BTO[gene][BTO]['evidence'] = evidence
                self.gene2BTO[gene][BTO]['confidence'] = confidence
                self.BTO2name[BTO] = BTO_name

            f.close()

        elif type_file == 'textmining':

            for line in f:
                # Split the line in fields
                # Snord14d  Snord14d    BTO:0001042 Amygdala    GNF V3  16955 expression units  2
                (gene, synonym, BTO, BTO_name, score, confidence, url) = line.strip().split("\t")

                self.genes.add(gene)

                if gene not in self.gene2synonym:
                    self.gene2synonym[gene] = synonym
                else:
                    if self.gene2synonym[gene] != synonym:
                        print('Gene {} with two different synonyms: {} and {}'.format(gene, synonym, self.gene2synonym[gene]))
                        sys.exit(10)

                self.gene2BTO.setdefault(gene, {})
                self.gene2BTO[gene].setdefault(BTO, {})
                self.gene2BTO[gene][BTO]['source'] = 'text-mining'
                self.gene2BTO[gene][BTO]['confidence'] = confidence
                self.BTO2name[BTO] = BTO_name

            f.close()


        return



if __name__ == "__main__":
    main()