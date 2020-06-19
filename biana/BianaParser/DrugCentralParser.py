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

        # Add these attributes as a valid external entity attribute since they are not recognized by BIANA
        self.biana_access.add_valid_external_entity_attribute_type( name = "DrugCentralID",
                                                                    data_type = "varchar(10)",
                                                                    category = "eE identifier attribute")

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


        ################################################
        #### CHECK DRUGS WITH MULTIPLE DRUGBANK IDS ####
        ################################################

        print("\n.....CHECKING DRUGS WITH MULTIPLE DRUGBANK IDS.....\n")

        drugs_skipped = set()

        for drug_name in parser.drug_names:

            drugbank_ids = parser.drugname_to_id[drug_name]
            if len(drugbank_ids) > 1:
                unique_drugs = set()
                for drugbank_id in drugbank_ids:
                    drug_name_types = parser.drugname_to_id_to_types[drug_name][drugbank_id]
                    if 'unique' in drug_name_types:
                        unique_drugs.add(drugbank_id)
                if len(unique_drugs) == 1:
                    parser.drugname_to_id[drug_name] = unique_drugs
                    unique_drugbank_id = list(unique_drugs)[0]
                    print('Drug {} NOT skipped because it has one unique DrugBankID: {}'.format(drug_name, unique_drugbank_id))
                else:
                    print('Drug {} skipped because it has more than 1 DrugBankIDs'.format(drug_name))
                    print(drugbank_ids)
                    drugs_skipped.add(drug_name)
    

        #########################################
        #### INSERT PARSED DATABASE IN BIANA ####
        #########################################

        print("\n.....INSERTING THE DRUGS IN THE DATABASE.....\n")

        self.external_entity_ids_dict = {}
        drugs_inserted = set()

        for drug_name in parser.drug_names:

            # Create an external entity corresponding to the drug in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(drug_name) and drug_name not in drugs_skipped:

                self.create_drug_external_entity(parser, drug_name)
                drugs_inserted.add(drug_name)

        print('{} drugs inserted'.format(len(drugs_inserted)))
        print('{} drugs skipped'.format(len(drugs_skipped)))


        print("\n.....INSERTING THE PROTEINS IN THE DATABASE.....\n")

        for uniprot_accession in parser.targets:

            # Create an external entity corresponding to the drug in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(uniprot_accession):

                self.create_protein_external_entity(parser, uniprot_accession)
                #print(uniprot_accession)

        print('{} proteins inserted'.format(len(parser.targets)))


        print("\n.....INSERTING THE INTERACTIONS IN THE DATABASE.....\n")

        interactions_inserted = set()

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
            interactions_inserted.add(interaction)

        print('{} interactions in total'.format(len(parser.interactions)))
        print('{} interactions inserted'.format(len(interactions_inserted)))

        return


    def create_drug_external_entity(self, parser, drug_name):
        """
        Create an external entity of a drug
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "drug" )

        # Annotate DRUGBANKID
        drugbank_ids = parser.drugname_to_id[drug_name]
        if len(drugbank_ids) > 1:
            print('Drug {} skipped during insertion of entity! it has more than 1 DrugBankIDs'.format(drug_name))
            print(drugbank_ids)
            sys.exit(10)
        else:
            for drugbank_id in drugbank_ids:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DrugBankID", value=drugbank_id, type="cross-reference") )

        # Annotate NAME
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Name", value=drug_name, type="cross-reference") )

        # Annotate DrugCentralID
        if drug_name in parser.drugname_to_drugcentral_id:
            drugcentral_id = parser.drugname_to_drugcentral_id[drug_name]
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DrugCentralID", value=drugcentral_id, type="unique") )

        # Annotate SMILES
        if drug_name in parser.drugname_to_smiles:
            for smiles in parser.drugname_to_smiles[drug_name]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "SMILES", value=smiles, type="unique") )

        # Annotate InChIKey
        if drug_name in parser.drugname_to_inchikey:
            for inchikey in parser.drugname_to_inchikey[drug_name]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "InChIKey", value=inchikey, type="unique") )

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
        self.smiles_file = os.path.join(input_dir, 'structures.smiles.tsv')

        self.drugname_to_id = {}
        self.drugname_to_drugcentral_id = {}
        self.drugname_to_id_to_types = {}
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
        self.drugname_to_smiles = {}
        self.drugname_to_inchikey = {}

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
                self.drugname_to_id_to_types.setdefault(drug_name, {})
                self.drugname_to_id_to_types[drug_name].setdefault(drug_id, set()).add(type_name)

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
                drugcentral_id = row[1]
                target_name = row[2]
                target_class = row[3].lower()
                uniprot_accessions = row[4].upper()
                gene_symbol = row[5]
                uniprot_entries = row[6].upper()
                action_type = row[17].lower()
                druggability_level = row[18].lower()
                organism = row[19].lower()
                #print(drug_name, drugcentral_id, uniprot_accessions, druggability_level, organism)

                # Skip the entries that are not in the identifiers file
                if drug_name not in self.drugname_to_id:
                    drugs_lost.add(drug_name)
                    print('Drug not found in identifiers file: {}'.format(drug_name))
                    continue
                else:
                    #print('Drug found in identifiers file!: {}'.format(drug_name))
                    drugs_mapped.add(drug_name)

                # Get the DrugCentral ID
                if not drug_name in self.drugname_to_drugcentral_id:
                    self.drugname_to_drugcentral_id[drug_name] = drugcentral_id
                else:
                    if drugcentral_id != self.drugname_to_drugcentral_id[drug_name]:
                        print('Two IDs for the drug {}:'.format(drug_name))
                        print('{} and {}'.format(self.drugname_to_drugcentral_id[drug_name], drugcentral_id))
                        sys.exit(10)

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


        print("\n.....PARSING SMILES FILE.....\n")

        with open(self.smiles_file,'r') as smiles_file_fd:

            first_line = smiles_file_fd.readline()
            #SMILES	InChI	InChIKey	ID	INN	CAS_RN
            #CCCCN1CCCC[C@H]1C(=O)NC1=C(C)C=CC=C1C	InChI=1S/C18H28N2O/c1-4-5-12-20-13-7-6-11-16(20)18(21)19-17-14(2)9-8-10-15(17)3/h8-10,16H,4-7,11-13H2,1-3H3,(H,19,21)/t16-/m0/s1	LEBVLXFERQHONN-INIZCTEOSA-N	4	levobupivacaine	27262-47-1

            csvreader = csv.reader(smiles_file_fd, delimiter='\t')
            for row in csvreader:
                smiles = row[0]
                inchi = row[1]
                inchikey = row[2]
                drugcentral_id = row[3]
                drug_name = row[4].lower()
                cas_rn = row[5]

                # Skip the entries that are not in the identifiers file
                if drug_name not in self.drugname_to_id:
                    continue
                #print(drugcentral_id, drug_name, smiles, inchikey)

                if drug_name != '' and drugcentral_id != '':
                    self.drugname_to_drugcentral_id[drug_name] = drugcentral_id
                    if smiles != '':
                        self.drugname_to_smiles.setdefault(drug_name, set()).add(smiles)
                    if inchikey != '':
                        self.drugname_to_inchikey.setdefault(drug_name, set()).add(inchikey)

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

