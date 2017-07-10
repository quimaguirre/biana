from bianaParser import *
import copy
                    
class IntactMutationsParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from the IntAct Mutations file

    """                 
                                                                                         
    name = "intact_mutations"
    description = "This file implements a program that fills up tables in BIANA database from data in the IntAct mutations dataset"
    external_entity_definition = "An external entity represents a mutated protein"
    external_entity_relations = "An external relation represents a interaction (or no interaction)"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "IntAct mutations parser",  
                             default_script_name = "intactMutationsParser.py",
                             default_script_description = IntactMutationsParser.description,
                             additional_compulsory_arguments = [])
                    
    def parse_database(self):
        """                                                                          
        Method that implements the specific operations of a MyData formatted file
        """


        # Check that the input path exists
        if os.path.isdir(self.input_file):
            self.input_path = self.input_file
        else:
            raise ValueError("You must specify a path instead of a file")

        # Check that the necessary files exist
        self.mutations_file = self.input_path + '/mutations.tsv'
        self.sequences_file = self.input_path + '/intact.fasta'

        if not os.path.isfile(self.mutations_file):
            print('The file \'mutations.tsv\' is not in the input path!')
            sys.exit(10)

        if not os.path.isfile(self.sequences_file):
            print('The file \'intact.fasta\' is not in the input path!')
            sys.exit(10)


        #### Add new external entity attributes in BIANA ####

        self.biana_access.add_valid_external_entity_attribute_type( name = "MutationType",
                                                                        data_type = "varchar(100)",
                                                                        category = "eE identifier attribute")

        # Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
        self.biana_access.refresh_database_information()



        # Parse the database
        parser = IntAct_mutations(self.input_path)
        parser.parse()



        # Defining the dict in which we will store the created external entities
        self.external_entity_ids_dict = {}


        print("\n.....OBTAINING THE MUTATED SEQUENCES.....\n")

        self.mutant2sequence = {}
        for uniprot_mutant in parser.uniprot_mutants:
            for intact_mutant in parser.uniprot2intact_mutant[uniprot_mutant]:
                mut_sequence = self.obtain_mutant_sequence(parser, uniprot_mutant, intact_mutant)
                self.mutant2sequence[intact_mutant] = mut_sequence


        print("\n.....INSERTING THE PROTEINS IN THE DATABASE.....\n")

        for uniprot in parser.uniprot_no_mutants:

            if not self.external_entity_ids_dict.has_key(uniprot):

                print("Adding non mutant protein %s" %(uniprot))
                self.create_non_mutant_external_entity(parser, uniprot) 

        for uniprot in parser.uniprot_mutants:

            # For each mutation we create a protein entity!
            for intact_mutant in parser.uniprot2intact_mutant[uniprot]:

                if not self.external_entity_ids_dict.has_key(intact_mutant):

                    print("Adding mutant protein %s" %(intact_mutant))
                    self.create_mutant_external_entity(parser, uniprot, intact_mutant)



        print("\n.....INSERTING THE INTERACTIONS IN THE DATABASE.....\n")

        for interaction in parser.interaction2uniprots.keys():

            print("Adding interaction: {}".format(interaction))
            self.create_interaction_entity(parser, interaction)



        print("\nPARSING OF INTACT MUTATIONS FINISHED. THANK YOU FOR YOUR PATIENCE\n")

        return



    def create_non_mutant_external_entity(self, parser, uniprot):
        """
        Create an external entity of a non-mutant protein and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        # Annotate its Uniprot Accession
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotAccession", value=uniprot, type="cross-reference") )

        # Annotate its Taxonomy ID
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier = "taxID", value = parser.uniprot2species[uniprot], type = "cross-reference" ) )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[uniprot] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_mutant_external_entity(self, parser, uniprot, intact):
        """
        Create an external entity of a mutant protein and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        # Annotate its Uniprot Accession
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotAccession", value=uniprot, type="cross-reference") )

        # Annotate its Taxonomy ID
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier = "taxID", value = parser.uniprot2species[uniprot], type = "cross-reference" ) )

        # Annotate the IntAct of the mutant
        intact_abbr = intact.split('EBI-')[1]
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "IntAct", value=intact_abbr, type="unique") )

        # Annotate the Protein Sequence
        mut_sequence = self.mutant2sequence[intact]
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "ProteinSequence", value=ProteinSequence(mut_sequence), type="unique") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[intact] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_interaction_entity(self, parser, interaction):
        """
        Create an external entity relation of an interaction and add it in BIANA
        """

        # CREATE THE EXTERNAL ENTITY RELATION
        # Create an external entity relation corresponding to a protein-protein interaction
        new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "interaction" )

        uniprot_mutant = parser.interaction2uniprot_mutant[interaction]
        intact_mutant = parser.interaction2intact_mutant[interaction]
        type_mutation = parser.intact2muttype[intact_mutant]
        # mutation disrupting strength(MI:1128)
        type_mutation = type_mutation.split('(MI:')[0]

        # Add the proteins in the interaction
        for uniprot in parser.interaction2uniprots[interaction]:
            if uniprot == uniprot_mutant:
                new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[intact_mutant] )
            else:
                new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[uniprot] )

        new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "MutationType",
                                                                                                         value = type_mutation, type = "unique" ) )

        # Insert this external entity relation into database
        self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return


    def obtain_mutant_sequence(self, parser, uniprot_mutant, intact_mutant):
        """ 
        Obtain mutant sequence
        """

        ori_sequence = parser.protein2sequence[uniprot_mutant]
        mut_sequence = ori_sequence

        for mutation in parser.intact2mutation2range[intact_mutant]:

            ori = parser.intact2mutation2ori[intact_mutant][mutation]
            mut = parser.intact2mutation2mut[intact_mutant][mutation].upper()
            ini, end = parser.intact2mutation2range[intact_mutant][mutation].split('-')

            if ori != ori_sequence[int(ini)-1:int(end)]:
                print('Error in mutant intact {}, uniprot {}. Original segment is not in sequence!'.format(intact_mutant, uniprot_mutant))
                sys.exit(10)

            mut_sequence = mut_sequence[0:int(ini)-1] + mut + mut_sequence[int(end):]

        return mut_sequence





class IntAct_mutations(object):

    def __init__(self, path):

        self.mutations_file = path + '/mutations.tsv'
        self.sequences_file = path + '/intact.fasta'

        self.protein2sequence = {}

        ###################
        self.interactions = set() # Set of intacts of the interactions
        self.interaction2uniprots = {}

        self.uniprot2species = {}
        self.interaction2intact_mutant = {}
        self.interaction2uniprot_mutant = {}

        self.all_uniprots = set()
        self.uniprot_mutants = set() # Set of uniprots of the mutants
        self.uniprot_no_mutants = set()
        self.uniprot2intact_mutant = {} # From uniprot to intact of the mutant. One uniprot can have multiple mutants 

        # Every mutant (described by intact) can have several mutations (described by mutation labels)
        self.intact2mutation2range = {}
        self.intact2mutation2ori = {}
        self.intact2mutation2mut = {}
        self.intact2mutation2pubmed = {}
        self.intact2muttype = {}



        return



    def parse(self):

        print("\n.....PARSING THE INTACT SEQUENCES FILE.....\n")

        fasta_file_fd = open(self.sequences_file,'r')

        intact_id_regex = re.compile(">INTACT:EBI-([0-9]+)\s(([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2})\-*(PRO_)*[0-9]*)\s\S+")
        # >INTACT:EBI-1000337 Q8AWF5 ndc80_xenla ndc80
        # >INTACT:EBI-823934 EBI-823934 q8id83_plaf7 q8id83_plaf7
        # >INTACT:EBI-2507565 UPI0000129A1B ipi00294632 ipi00294632
        # >INTACT:EBI-2507556 XP_293026 ipi00260209 ipi00260209
        # >INTACT:EBI-2363428 A0A1B0GR91+ENSMUSG00000045010 a0a1b0gr91_mouse Gm4779

        uniprot_id = None
        sequence = ''
        skip = False

        for line in fasta_file_fd:

            if line[0] == '>':
                if len(sequence)>0:
                    self.protein2sequence[uniprot_id] = sequence
                skip = False
                sequence = ''
                m = intact_id_regex.match(line)
                if m:
                    fields = line.strip().split('\t')
                    intact_id = m.group(1)
                    uniprot_id = m.group(2)
                else:
                    skip = True # We skip the proteins that are not Uniprot, and therefore not recognized by the regex
            else:
                if skip == False:
                    sequence = sequence + line.strip()

        if len(sequence)>0:
            self.protein2sequence[uniprot_id] = sequence

        fasta_file_fd.close()

        print("\n.....PARSING OF THE INTACT SEQUENCES FINISHED!\n")



        print("\n.....PARSING THE INTACT MUTATIONS FILE.....\n")

        mutations_file_fd = open(self.mutations_file,'r')

        first_line = mutations_file_fd.readline()[1:]

        # Obtain a dictionary: "field_name" => "position"
        fields_dict = self.obtain_header_fields(first_line)
        #Feature AC Feature short label Feature range(s)    Original sequence   Resulting sequence  Feature type    Feature annotation  Affected protein AC Affected protein symbol Affected protein full name  Affected protein organism   Interaction participants    PubMedID    Figure legend   Interaction AC


        for line in mutations_file_fd:

            # Split the line in fields
            fields = line.strip().split("\t")

            # Obtain the fields of interest
            intact_mutant = fields[ fields_dict['Feature AC'] ] # In this case, the IntAct corresponds to a protein
            mutation_label = fields[ fields_dict['Feature short label'] ] # Label of the mutation, i.e. ser94ala
            mutation_range = fields[ fields_dict['Feature range(s)'] ] # Range of mutation i.e. 94-94
            original_seq = fields[ fields_dict['Original sequence'] ] # Aminoacid(s) in the original sequence i.e. S
            mutation_seq = fields[ fields_dict['Resulting sequence'] ] # Aminoacid(s) in the mutant sequence i.e. A
            mutation_type = fields[ fields_dict['Feature type'] ] # Type of mutation (i.e. increasing(MI:0382))
            uniprot_protein_affected = fields[ fields_dict['Affected protein AC'] ] # Uniprot
            species = fields[ fields_dict['Affected protein organism'] ] # i.e. 9606 - Homo sapiens
            interaction = fields[ fields_dict['Interaction participants'] ] # uniprotkb:P35222(protein(MI:0326), 9606 - Homo sapiens);uniprotkb:P46937(protein(MI:0326), 9606 - Homo sapiens)
            pubmed = fields[ fields_dict['PubMedID'] ]
            intact_interaction = fields[ fields_dict['Interaction AC'] ] # In this other case, the IntAct corresponds to an interaction

            type_prot, uniprot_protein_affected = uniprot_protein_affected.split(':')

            # We will skip the mutant proteins that are not Uniprot accessions (there are IntAct ones)
            if type_prot != 'uniprotkb':
                continue

            if uniprot_protein_affected not in self.protein2sequence:
                print('Uniprot {} does not have sequence!'.format(uniprot_protein_affected))
                continue
            else:
                ori_sequence = self.protein2sequence[uniprot_protein_affected]
                ini, end = mutation_range.split('-')
                if original_seq != ori_sequence[int(ini)-1:int(end)]:
                    print('Original sequence does not correspond to original segment in intact mutation {}, uniprot {}'.format(intact_mutant, uniprot_protein_affected))
                    continue

            self.interactions.add(intact_interaction)
            self.interaction2intact_mutant[intact_interaction] = intact_mutant # Add the intact of the mutant
            self.interaction2uniprot_mutant[intact_interaction] = uniprot_protein_affected # Add the uniprot of the mutant

            interaction_regex = re.compile("uniprotkb:(([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2})\-*(PRO_)*[0-9]*)\(\w+\(MI:[0-9]{4}\)\,\s([0-9]+)\s\S+")
            interaction = interaction.split(';')
            for participant in interaction:
                m = interaction_regex.match(participant)
                if m:
                    uniprot_id = m.group(1)
                    taxid = m.group(5)
                    self.all_uniprots.add(uniprot_id)
                    if uniprot_id != uniprot_protein_affected:
                        self.uniprot_no_mutants.add(uniprot_id)
                    self.interaction2uniprots.setdefault(intact_interaction, set())
                    self.interaction2uniprots[intact_interaction].add(uniprot_id) # Add the uniprots of the participants in the interaction
                    self.uniprot2species[uniprot_id] = taxid # Add the TaxID of the uniprot

            self.uniprot_mutants.add(uniprot_protein_affected)
            self.uniprot2intact_mutant.setdefault(uniprot_protein_affected, set()) # Every uniprot can have different intact mutants
            self.uniprot2intact_mutant[uniprot_protein_affected].add(intact_mutant)

            self.intact2mutation2range.setdefault(intact_mutant, {})
            self.intact2mutation2ori.setdefault(intact_mutant, {})
            self.intact2mutation2mut.setdefault(intact_mutant, {})
            self.intact2mutation2pubmed.setdefault(intact_mutant, {})

            self.intact2mutation2range[intact_mutant][mutation_label] = mutation_range
            self.intact2mutation2ori[intact_mutant][mutation_label] = original_seq
            self.intact2mutation2mut[intact_mutant][mutation_label] = mutation_seq
            self.intact2mutation2pubmed[intact_mutant][mutation_label] = pubmed
            if intact_mutant in self.intact2muttype:
                if mutation_type != self.intact2muttype[intact_mutant]:
                    print('2 types of mutation in the same IntAct mutant: {}'.format(intact_mutant))
                    sys.exit(10)
            else:
                self.intact2muttype[intact_mutant] = mutation_type


        mutations_file_fd.close()

        print("\n.....PARSING OF THE INTACT MUTATIONS FILE FINISHED!\n")

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