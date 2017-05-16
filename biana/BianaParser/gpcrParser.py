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


        # Method name to psi-mi obo ID
        self.method2psimi = {
            'bioluminescence resonance energy transfer' : 0012,
            'ubiquitin reconstruction' : 0112
        }

        # Species name to Taxonomy ID
        self.species2taxid = {
            'human' : 9606
        }

        # Parse the database
        parser = GPCR_Interactome(self.input_file)
        parser.parse()

        print('The number of interactions is: {}'.format(len(parser.interaction2uniprot.keys())))
        print('The number of uniprot proteins is: {}'.format(len(parser.uniprots)))
        print('The number of gene symbols is: {}'.format(len(set(parser.uniprot2genesymbol.values()))))

        # Check that the taxonomy ids for the species are in the dictionary species2taxid
        for interaction in parser.interaction2species:
            for species in parser.interaction2species[interaction]:
                if species not in self.species2taxid:
                    print('The species {} is not in the dictionary species2taxid.\nPlease, search the Taxonomy ID of the species in www.uniprot.org/taxonomy and add it in the dictionary'.format(species))
                    sys.exit(10)

        # Check that the psi-mi obo ids for the methods are in the dictionary method2psimi
        for interaction in parser.interaction2method:
            for method in parser.interaction2method[interaction]:
                if method not in self.method2psimi:
                    print('The method {} is not in the dictionary method2psimi.\nPlease, search the PSI-MI ID of the method in http://obo.cvs.sourceforge.net/viewvc/obo/obo/ontology/genomic-proteomic/protein/psi-mi.obo and add it in the dictionary'.format(method))
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
            self.create_interaction_entity(parser, interaction)



        print("\nPARSING OF GPCRs INTERACTOME FINISHED. THANK YOU FOR YOUR PATIENCE\n")

        return



    def create_protein_external_entity(self, parser, uniprot):
        """
        Create an external entity of a protein and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        # Annotate its Uniprot Accession
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotAccession", value=uniprot, type="cross-reference") )

        # Associate its GeneSymbol
        if uniprot in parser.uniprot2genesymbol:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneSymbol", value=parser.uniprot2genesymbol[uniprot].upper(), type="cross-reference") )
        else:
            print("Gene Symbol not available for %s" %(uniprot))
            pass

        for interaction in parser.interaction2uniprot:
            for prot in parser.interaction2uniprot[interaction]:
                if prot == uniprot:
                    for species in parser.interaction2species[interaction]:
                        if species in self.species2taxid:
                            taxID = self.species2taxid[species]
                            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier = "taxID",
                                                                                        value = taxID, type = "cross-reference" ) )
                    break

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[uniprot] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_interaction_entity(self, parser, interaction):
        """
        Create an external entity relation of an interaction and add it in BIANA
        """

        # CREATE THE EXTERNAL ENTITY RELATION
        # Create an external entity relation corresponding to a protein-protein interaction
        new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "interaction" )

        # Add the proteins in the interaction
        for uniprot in parser.interaction2uniprot[interaction]:
            new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[uniprot] )

        # Add the species of the interaction
        if interaction in parser.interaction2species:
            for species in parser.interaction2species[interaction]:
                if species in self.species2taxid:
                    taxID = self.species2taxid[species]
                    new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "taxID",
                                                                                                                     value = taxID, type = "cross-reference" ) )
                else:
                    print('The species {} is not in the dictionary species2taxid.\nPlease, search the Taxonomy ID of the species in www.uniprot.org/taxonomy and add it in the dictionary'.format(species))
                    sys.exit(10)
        else:
            print("Species not available for {}".format(interaction))
            sys.exit(10)


        # Add the method used to report the interaction
        if interaction in parser.interaction2method:
            for method in parser.interaction2method[interaction]:
                if method in self.method2psimi:
                    psimi = self.method2psimi[method]
                    new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "method_id",
                                                                                                                     value = psimi) )
                else:
                    print('The method {} is not in the dictionary method2psimi.\nPlease, search the PSI-MI ID of the method in http://obo.cvs.sourceforge.net/viewvc/obo/obo/ontology/genomic-proteomic/protein/psi-mi.obo and add it in the dictionary'.format(method))
                    sys.exit(10)
        else:
            print("Method not available for {}".format(interaction))
            sys.exit(10)

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

        print("\n.....PARSING THE GPCRs INTERACTOME.....\n")

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
            interaction_1 = uniprot1 + '---' + uniprot2
            interaction_2 = uniprot2 + '---' + uniprot1

            # Insert the fields into dictionaries
            self.uniprots.add(uniprot1)
            if uniprot1 not in self.uniprot2genesymbol:
                self.uniprot2genesymbol[uniprot1] = symbol1
            else:
                # Check that there are not different gene symbols for one uniprot
                if self.uniprot2genesymbol[uniprot1] != symbol1:
                    print('Different gene symbols for uniprot {}'.format(uniprot1))
                    sys.exit(10)

            self.uniprots.add(uniprot2)
            if uniprot2 not in self.uniprot2genesymbol:
                self.uniprot2genesymbol[uniprot2] = symbol2
            else:
                # Check that there are not different gene symbols for one uniprot
                if self.uniprot2genesymbol[uniprot2] != symbol2:
                    print('Different gene symbols for uniprot {}'.format(uniprot2))
                    sys.exit(10)

            if not interaction_1 in self.interaction2uniprot:
                if not interaction_2 in self.interaction2uniprot:
                    interaction = interaction_1
                else:
                    interaction = interaction_2
            else:
                interaction = interaction_1
            self.interaction2uniprot.setdefault(interaction, set())
            self.interaction2species.setdefault(interaction, set())
            self.interaction2evidence.setdefault(interaction, set())
            self.interaction2method.setdefault(interaction, set())

            self.interaction2uniprot[interaction].add(uniprot1)
            self.interaction2uniprot[interaction].add(uniprot2)
            self.interaction2species[interaction].add(specie)
            self.interaction2evidence[interaction].add(evidence_type)
            self.interaction2method[interaction].add(detection_method)

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