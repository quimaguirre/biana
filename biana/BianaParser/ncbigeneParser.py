from bianaParser import *
import gzip
                    
class ncbigeneParser(BianaParser):                                                        
    """             
    ncbigeneParser Class 

    Parses data from the database NCBI Gene

    """                 
                                                                                         
    name = "ncbigene"
    description = "This file implements a program that fills up tables in BIANA database from data in NCBI Gene"
    external_entity_definition = "An external entity represents a gene"
    external_entity_relations = "There are no relations in the database"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "NCBI Gene parser",  
                             default_script_name = "ncbigeneParser.py",
                             default_script_description = ncbigeneParser.description,     
                             additional_compulsory_arguments = [])
                    
    def parse_database(self):
        """                                                                              
        Method that implements the specific operations of a MyData formatted file
        """

        #####################################
        #### DEFINE NEW BIANA CATEGORIES ####
        #####################################

        # Add GO_evidence as a valid external entity attribute since it is not recognized by BIANA
        self.biana_access.add_valid_external_entity_attribute_type( name = "GO_evidence",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE identifier attribute")

        # Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
        self.biana_access.refresh_database_information()



        ###########################
        #### CHECK INPUT FILES ####
        ###########################

        # Check that the input is a path
        if os.path.isdir(self.input_file):
            self.input_path = self.input_file
        else:
            raise ValueError("You must specify a path instead of a file")

        # Check that all the needed files exist
        files_list = [os.path.join(self.input_path, f) for f in os.listdir(self.input_path) if os.path.isfile(os.path.join(self.input_path, f))]
        files = ["gene2ensembl.gz", "gene2go.gz"]

        for current_file in files:
            file_path = os.path.join(self.input_path, current_file)
            if file_path not in files_list:
                raise ValueError("File %s is missing in %s" %(current_file, self.input_path))



        ########################
        #### PARSE DATABASE ####
        ########################

        # Parse the database
        parser = NCBIGene(self.input_path)
        parser.parse()

        #print(parser.geneid2accession)
        #print(parser.geneid2ensembl)



        #########################################
        #### INSERT PARSED DATABASE IN BIANA ####
        #########################################

        print("\n.....INSERTING THE GENES IN THE DATABASE.....\n")

        # Defining the dict in which we will store the created external entities
        self.external_entity_ids_dict = {}

        for geneid in parser.geneIDs:

            # Create an external entity corresponding to the geneID in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(geneid):

                #print("Adding gene %s" %(geneid))
                self.create_gene_external_entity(parser, geneid) 


        print("\nPARSING OF NCBI GENE FINISHED. THANK YOU FOR YOUR PATIENCE\n")

        return



    def create_gene_external_entity(self, parser, geneid):
        """
        Create an external entity of a gene and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        # Annotate its GeneID
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneID", value=geneid, type="unique") )

        # Annotate its TaxID
        taxid = parser.geneid2taxid[geneid]
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "TaxID", value=taxid, type="cross-reference") )

        # Associate its Ensembl
        if geneid in parser.geneid2ensembl:
            for type_id in parser.geneid2ensembl[geneid]:
                for ensembl in parser.geneid2ensembl[geneid][type_id]:
                    new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Ensembl", value=ensembl.upper(), type="cross-reference") )
        else:
            print("Ensembl not available for GeneID: %s" %(geneid))
            pass

        # Associate its Accession
        if geneid in parser.geneid2accession:
            for type_id in parser.geneid2accession[geneid]:
                for accession in parser.geneid2accession[geneid][type_id]:
                    if len(accession) > 1:
                        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "RefSeq", value=accession[0].upper(), version=accession[1], type="cross-reference") )
                    else:
                        print('CAUTION!!!: REFSEQ WITHOUT VERSION FOR GENEID {} ---> {}'.format(geneid, accession[0]))
                        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "RefSeq", value=accession[0].upper(), type="cross-reference") )
        else:
            print("Accession not available for GeneID: %s" %(geneid))
            pass

        # Associate its GO ID
        if geneid in parser.geneid2go:
            for go_id in parser.geneid2go[geneid]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GO", value=go_id, type="unique") )
                gene_go = '{}---{}'.format(geneid, go_id)
                if gene_go in parser.gene_go2evidences:
                    for evidence in parser.gene_go2evidences[gene_go]:
                        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GO_evidence", value=evidence, type="unique") )
                if gene_go in parser.gene_go2pubmeds:
                    for pubmed in parser.gene_go2pubmeds[gene_go]:
                        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Pubmed", value=pubmed, type="unique") )
        else:
            print("GO not available for GeneID: %s" %(geneid))
            pass

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[geneid] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return





class NCBIGene(object):

    def __init__(self, input_path):

        self.gene2ensembl_file = os.path.join(input_path, "gene2ensembl.gz")
        self.gene2go_file = os.path.join(input_path, "gene2go.gz")

        self.geneIDs = set()
        self.geneid2taxid = {}
        self.geneid2ensembl = {}
        self.geneid2accession = {}

        self.geneid2go = {}
        self.gene_go2evidences = {}
        self.gene_go2pubmeds = {}

        return



    def parse(self):


        #################################
        #### PARSE GENE2ENSEMBL FILE ####
        #################################

        print("\n.....PARSING THE FILE GENE2ENSEMBL.....\n")

        gene2ensembl_fd = gzip.open(self.gene2ensembl_file,'rb')

        first_line = gene2ensembl_fd.readline()

        # Obtain a dictionary: "field_name" => "position"
        fields_dict = self.obtain_header_fields(first_line[1:])
        #tax_id GeneID  Ensembl_gene_identifier RNA_nucleotide_accession.version    Ensembl_rna_identifier  protein_accession.version   Ensembl_protein_identifier

        for line in gene2ensembl_fd:

            # Split the line in fields
            fields = line.strip().split("\t")

            # Obtain the fields of interest
            taxid = fields[ fields_dict['tax_id'] ]
            geneid = fields[ fields_dict['GeneID'] ]
            ensembl_gene = fields[ fields_dict['Ensembl_gene_identifier'] ]
            accession_rna = fields[ fields_dict['RNA_nucleotide_accession.version'] ]
            ensembl_rna = fields[ fields_dict['Ensembl_rna_identifier'] ]
            accession_prot = fields[ fields_dict['protein_accession.version'] ]
            ensembl_prot = fields[ fields_dict['Ensembl_protein_identifier'] ]

            # Insert GeneID
            self.geneIDs.add(geneid)

            # Insert TaxID. There can only be one taxID for GeneID. If not, error
            if geneid not in self.geneid2taxid:
                self.geneid2taxid[geneid] = taxid
            else:
                if self.geneid2taxid[geneid] != taxid:
                    print('Different taxIDs for the GeneID: {}\nFirst taxID: {}  Second taxID: {}'.format(geneid, self.geneid2taxid[geneid], taxid))
                    sys.exit(10)

            # Insert Ensembl
            self.geneid2ensembl.setdefault(geneid, {})

            if ensembl_gene != '-':
                ensembl_gene = ensembl_gene.split('.')[0]
                self.geneid2ensembl[geneid].setdefault('gene', [])
                self.geneid2ensembl[geneid]['gene'].append(ensembl_gene)
            if ensembl_rna != '-':
                ensembl_rna = ensembl_rna.split('.')[0]
                self.geneid2ensembl[geneid].setdefault('rna', [])
                self.geneid2ensembl[geneid]['rna'].append(ensembl_rna)
            if ensembl_prot != '-':
                ensembl_prot = ensembl_prot.split('.')[0]
                self.geneid2ensembl[geneid].setdefault('protein', [])
                self.geneid2ensembl[geneid]['protein'].append(ensembl_prot)

            # Insert Accession
            self.geneid2accession.setdefault(geneid, {})

            if accession_rna != '-':
                self.geneid2accession[geneid].setdefault('rna', [])
                self.geneid2accession[geneid]['rna'].append(accession_rna.split('.'))
            if accession_prot != '-':
                self.geneid2accession[geneid].setdefault('protein', [])
                self.geneid2accession[geneid]['protein'].append(accession_prot.split('.'))

        gene2ensembl_fd.close()


        ############################
        #### PARSE GENE2GO FILE ####
        ############################

        print("\n.....PARSING THE FILE GENE2GO.....\n")

        gene2go_fd = gzip.open(self.gene2go_file,'rb')

        first_line = gene2go_fd.readline()

        # Obtain a dictionary: "field_name" => "position"
        fields_dict = self.obtain_header_fields(first_line[1:])
        #tax_id GeneID  GO_ID   Evidence    Qualifier   GO_term PubMed  Category

        for line in gene2go_fd:

            # Split the line in fields
            fields = line.strip().split("\t")

            # Obtain the fields of interest
            taxid = fields[ fields_dict['tax_id'] ]
            geneid = fields[ fields_dict['GeneID'] ]
            go_id = fields[ fields_dict['GO_ID'] ].upper()
            evidence = fields[ fields_dict['Evidence'] ].upper()
            qualifier = fields[ fields_dict['Qualifier'] ].lower()
            go_term = fields[ fields_dict['GO_term'] ].lower()
            pubmed = fields[ fields_dict['PubMed'] ]
            category = fields[ fields_dict['Category'] ].lower()

            # Insert GeneID
            self.geneIDs.add(geneid)

            # Insert TaxID. There can only be one taxID for GeneID. If not, error
            if geneid not in self.geneid2taxid:
                self.geneid2taxid[geneid] = taxid
            else:
                if self.geneid2taxid[geneid] != taxid:
                    print('Different taxIDs for the GeneID: {}\nFirst taxID: {}  Second taxID: {}'.format(geneid, self.geneid2taxid[geneid], taxid))
                    sys.exit(10)

            # Insert GO
            go_id = go_id.split('GO:')[1]
            self.geneid2go.setdefault(geneid, set())
            self.geneid2go[geneid].add(go_id)

            if evidence != '-':
                gene_go = '{}---{}'.format(geneid, go_id)
                self.gene_go2evidences.setdefault(gene_go, set())
                self.gene_go2evidences[gene_go].add(evidence)
            else:
                print('No evidence for GeneID {} and GO ID {}!'.format(geneid, go_id))
                sys.exit(10)

            if pubmed != '-':
                gene_go = '{}---{}'.format(geneid, go_id)
                self.gene_go2pubmeds.setdefault(gene_go, set())
                self.gene_go2pubmeds[gene_go].add(pubmed)

        gene2go_fd.close()


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