from bianaParser import *
                    
class DrugCentralParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from DrugCentral

    """                 
                                                                                         
    name = "DrugCentral"
    description = "This file implements a program that fills up tables in BIANA database from data in DrugCentral"
    external_entity_definition = "An external entity represents a drug or a protein"
    external_entity_relations = "An external relation represents a drug combination or an interaction between drugs or drug-target"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "DrugCentral parser",  
                             default_script_name = "DrugCentralParser.py",
                             default_script_description = DrugCentralParser.description,     
                             additional_compulsory_arguments = [])
        self.default_eE_attribute = "drugbankid"
                    
    def parse_database(self):
        """                                                                              
        Method that implements the specific operations of a MyData formatted file
        """

        #####################################
        #### DEFINE NEW BIANA CATEGORIES ####
        #####################################

        # Add DrugBankID as a valid external entity attribute since it is not recognized by BIANA
        self.biana_access.add_valid_external_entity_attribute_type( name = "DrugCentral_action",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "DrugCentral_druggability",
                                                                    data_type = "ENUM(\"tbio\",\"tclin\",\"tchem\",\"tdark\")",
                                                                    category = "eE descriptive attribute")

        # Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
        self.biana_access.refresh_database_information()



        ###########################
        #### CHECK INPUT FILES ####
        ###########################

        # Check that all the files exist
        if not os.path.isdir(self.input_file):
            raise ValueError('You must specify a directory!')



        ########################
        #### PARSE DATABASE ####
        ########################

        parser = DrugCentral(self.input_file)
        parser.parse()


        #########################################
        #### INSERT PARSED DATABASE IN BIANA ####
        #########################################

        print("\n.....INSERTING THE DRUGS IN THE DATABASE.....\n")

        self.external_entity_ids_dict = {}
        drugs_skipped = []

        for drug_name in parser.drug_names:

            # Create an external entity corresponding to the drug in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(drug_name):

                drug_skipped = self.create_drug_external_entity(parser, drug_name)
                if drug_skipped:
                    drugs_skipped.append(drug_skipped)
                #print(drug_name)

        print('{} drugs inserted'.format(len(parser.drug_names)))
        print('{} drugs skipped'.format(len(drugs_skipped)))


        print("\n.....INSERTING THE PROTEINS IN THE DATABASE.....\n")

        for uniprot_accession in parser.targets:

            # Create an external entity corresponding to the drug in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(uniprot_accession):

                self.create_protein_external_entity(parser, uniprot_accession)
                #print(uniprot_accession)

        print('{} proteins inserted'.format(len(parser.targets)))


        print("\n.....INSERTING THE INTERACTIONS IN THE DATABASE.....\n")

        for interaction in parser.interactions:

            # CREATE THE EXTERNAL ENTITY RELATION
            # Create an external entity relation corresponding to interaction between the drug and the target
            new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "interaction" )

            (uniprot_accession, drug_name) = interaction
            #print(uniprot_accession, drug_name)

            # Skip if drug not considered
            if drug_name in drugs_skipped:
                continue

            # Associate the participants of the interaction
            new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[uniprot_accession] )
            new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[drug_name] )

            if interaction in parser.interaction_to_druggability:

                for druggability in parser.interaction_to_druggability[interaction]:

                    # Associate the interaction with the druggability
                    new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier= "DrugCentral_druggability", value=druggability, type="unique" ) )

            if interaction in parser.interaction_to_action:

                for action_type in parser.interaction_to_action[interaction]:

                    # Associate the interaction with the action
                    new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier= "DrugCentral_action", value=action_type, type="unique" ) )


            # Insert this external entity relation into database
            self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        print('{} interactions inserted'.format(len(parser.interactions)))

        return None


    def create_drug_external_entity(self, parser, drug_name):
        """
        Create an external entity of a drug
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "drug" )

        # Annotate DRUGBANKID
        drugbank_ids = parser.drugname_to_id[drug_name]
        if len(drugbank_ids) > 1:
            print('Drug {} skipped because it has more than 1 DrugBankIDs'.format(drug_name))
            print(drugbank_ids)
            return drug_name
        else:
            for drugbank_id in drugbank_ids:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DrugBankID", value=drugbank_id, type="cross-reference") )

        # Annotate NAME
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Name", value=drug_name, type="cross-reference") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[drug_name] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_protein_external_entity(self, parser, uniprot_accession):
        """
        Create an external entity of a protein
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        # Annotate it as uniprot_accession
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotAccession", value=uniprot_accession, type="cross-reference") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[uniprot_accession] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return



class DrugCentral(object):

    def __init__(self, input_dir):

        self.input_dir = input_dir
        self.interactions_file = os.path.join(input_dir, 'drug.target.interaction.tsv')
        self.identifiers_file = os.path.join(input_dir, 'drugbankID_to_names.txt')

        self.drugname_to_id = {}
        self.id_types = set()
        self.targets = set()
        self.drug_names = set()
        self.interactions = set()
        self.action_types = set()
        self.druggability_levels = set()
        self.interaction_to_action = {}
        self.interaction_to_druggability = {}
        self.target_classes = set()
        self.uniprot_to_class = {}

        return

    def parse(self):

        import csv

        print("\n.....PARSING IDENTIFIERS FILE.....\n")

        with open(self.identifiers_file, 'r') as identifiers_file_fd:

            first_line = identifiers_file_fd.readline()
            # DrugBankID    Drug name   Type of name
            csvreader = csv.reader(identifiers_file_fd, delimiter='\t')
            for row in csvreader:
                drug_id = row[0].upper()
                drug_name = row[1].lower()
                type_name = row[2].lower()
                #print(drug_id, drug_name, type_name)
                self.drugname_to_id.setdefault(drug_name, set())
                self.drugname_to_id[drug_name].add(drug_id)

        #print(self.id_types)
        #set(['mddb', 'unii', 'pubchem_cid', 'umlscui', 'nui', 'secondary_cas_rn', 'iuphar_ligand_id', 'vuid', 'nddf', 'chembl_id', 'pdb_chem_id', 'snomedct_us', 'mesh_descriptor_ui', 'mesh_supplemental_record_ui', 'mmx', 'drugbank_id', 'mmsl', 'vandf', 'ndfrt', 'rxnorm', 'chebi', 'kegg_drug', 'inn_id'])


        drugs_lost = set()
        drugs_mapped = set()

        print("\n.....PARSING INTERACTIONS FILE.....\n")

        with open(self.interactions_file,'r') as interactions_file_fd:

            first_line = interactions_file_fd.readline()
            # "DRUG_NAME"   "STRUCT_ID" "TARGET_NAME"   "TARGET_CLASS"  "ACCESSION" "GENE"  "SWISSPROT" "ACT_VALUE" "ACT_UNIT"  "ACT_TYPE"  "ACT_COMMENT"   "ACT_SOURCE"    "RELATION"  "MOA"   "MOA_SOURCE"    "ACT_SOURCE_URL"    "MOA_SOURCE_URL"    "ACTION_TYPE"   "TDL"   "ORGANISM"
            # "levobupivacaine" 4   "Sodium channel protein type 4 subunit alpha"   "Ion channel"   "P35499"    "SCN4A" "SCN4A_HUMAN"                   "WOMBAT-PK"     1   "CHEMBL"        "https://www.ebi.ac.uk/chembl/compound/inspect/CHEMBL1200749"   "BLOCKER"   "Tclin" "Homo sapiens"

            csvreader = csv.reader(interactions_file_fd, delimiter='\t')
            for row in csvreader:
                drug_name = row[0].lower()
                target_name = row[2]
                target_class = row[3].lower()
                uniprot_accessions = row[4].upper()
                gene_symbol = row[5]
                uniprot_entries = row[6].upper()
                action_type = row[17].lower()
                druggability_level = row[18].lower()
                organism = row[19].lower()
                #print(drug_name, uniprot_accessions, druggability_level, organism)

                # Skip the entries that are not in the identifiers file
                if drug_name not in self.drugname_to_id:
                    drugs_lost.add(drug_name)
                    print('Drug not found in identifiers file: {}'.format(drug_name))
                    continue
                else:
                    print('Drug found in identifiers file!: {}'.format(drug_name))
                    drugs_mapped.add(drug_name)

                # Skip the entries that do not have uniprot identifier for target
                if uniprot_accessions == '':
                    continue
                else:
                    uniprot_accessions = uniprot_accessions.split('|')

                # Skip the entries that do not have uniprot identifier for target
                if uniprot_entries == '':
                    continue
                else:
                    uniprot_entries = uniprot_entries.split('|')

                # Skip the entries without druggability level
                if druggability_level == '':
                    continue
                else:
                    druggability_level = druggability_level.split('|')

                # Skip if the organism is not human
                if organism != 'homo sapiens':
                    continue

                for uniprot_accession in uniprot_accessions:
                    interaction = (uniprot_accession, drug_name)
                    self.interactions.add(interaction)
                    self.targets.add(uniprot_accession)
                    self.drug_names.add(drug_name)

                    #print(drug_name, uniprot_accession)
                    #print(target_class, action_type, druggability_level)

                    if action_type != '':
                        self.action_types.add(action_type)
                        self.interaction_to_action.setdefault(interaction, set())
                        self.interaction_to_action[interaction].add(action_type)

                    for druggability in druggability_level:
                        self.druggability_levels.add(druggability)
                        self.interaction_to_druggability.setdefault(interaction, set())
                        self.interaction_to_druggability[interaction].add(druggability)

                    if target_class != '':
                        self.target_classes.add(target_class)
                        self.uniprot_to_class[uniprot_accession] = target_class

        #print(self.druggability_levels)
        #set(['tbio', 'tclin', 'tchem', 'tdark'])
        print('Number of drugs not mapped to DrugBank IDs: {}'.format(len(drugs_lost)))
        print('Number of drugs successfully mapped: {}'.format(len(drugs_mapped)))

        return



    def obtain_header_fields(self, first_line, separator='\t'):
        """ 
        Obtain a dictionary: "field_name" => "position" 
        """
        fields_dict = {}

        header_fields = first_line.strip().split(separator)
        for x in xrange(0, len(header_fields)):
            fields_dict[header_fields[x].lower()] = x

        return fields_dict


if __name__ == "__main__":
    main()

