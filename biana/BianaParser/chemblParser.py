from bianaParser import *
                    
class chemblParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from ChEMBL

    """                 
                                                                                         
    name = "chembl"
    description = "This file implements a program that fills up tables in BIANA database from data in ChEMBL"
    external_entity_definition = "An external entity represents a drug or a protein"
    external_entity_relations = "An external relation represents a drug combination or an interaction between drugs or drug-target"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "ChEMBL parser",  
                             default_script_name = "chemblParser.py",
                             default_script_description = chemblParser.description,     
                             additional_compulsory_arguments = [])
        self.default_eE_attribute = "chembl"
                    
    def parse_database(self):
        """                                                                              
        Method that implements the specific operations of a MyData formatted file
        """

        #####################################
        #### DEFINE NEW BIANA CATEGORIES ####
        #####################################

        # Add DrugBankID as a valid external entity attribute since it is not recognized by BIANA
        self.biana_access.add_valid_external_entity_attribute_type( name = "CHEMBL_drug_type",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE descriptive attribute")

        # self.biana_access.add_valid_external_entity_attribute_type( name = "CHEMBL_target_type",
        #                                                             data_type = "varchar(370)",
        #                                                             category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "CHEMBL_action",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "CHEMBL_mechanism",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE descriptive attribute")

        data_type_dict = {"fields": [ ("value","varchar(20)"),
                                      ("MESH_heading","varchar(370)",False)
                                    ],
                          "indices": ("value",)}

        self.biana_access.add_valid_external_entity_attribute_type( name = "CHEMBL_indication",
                                                                        data_type = data_type_dict,
                                                                        category = "eE special attribute")



        # Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
        self.biana_access.refresh_database_information()



        ###########################
        #### CHECK INPUT FILES ####
        ###########################

        # Check that all the files exist
        if os.path.isdir(self.input_file):
            self.input_path = self.input_file
        else:
            raise ValueError("You must specify a path instead of a file")

        onlyfiles = [f for f in os.listdir(self.input_path) if os.path.isfile(os.path.join(self.input_path, f))]
        for input_file in onlyfiles:
            if input_file.startswith('chembl_drugs'):
                drug_file = os.path.join(self.input_path, input_file)
            elif input_file.startswith('target'):
                target_file = os.path.join(self.input_path, input_file)
            elif input_file.startswith('chembl_drugtargets'):
                drugtarget_file = os.path.join(self.input_path, input_file)
            elif input_file.startswith('chembl_indications'):
                indication_file = os.path.join(self.input_path, input_file)


        ########################
        #### PARSE DATABASE ####
        ########################

        parser = ChEMBL(drug_file, target_file, drugtarget_file, indication_file)
        parser.parse()


        #########################################
        #### INSERT PARSED DATABASE IN BIANA ####
        #########################################

        print("\n.....INSERTING THE DRUGS IN THE DATABASE.....\n")

        self.external_entity_ids_dict = {}

        for drug_chembl in parser.drugs:

            # Create an external entity corresponding to the drug in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(drug_chembl):

                self.create_drug_external_entity(parser, drug_chembl)
                #print(drug_chembl)

        print('{} drugs inserted'.format(len(parser.drugs)))



        print("\n.....INSERTING THE PROTEINS IN THE DATABASE.....\n")

        for target_uniprot in parser.targets:

            # Create an external entity corresponding to the drug in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(target_uniprot):

                self.create_protein_external_entity(parser, target_uniprot)
                #print(target_uniprot)

        print('{} proteins inserted'.format(len(parser.targets)))



        print("\n.....INSERTING THE INTERACTIONS IN THE DATABASE.....\n")

        for interaction in parser.interactions:

            # CREATE THE EXTERNAL ENTITY RELATION
            # Create an external entity relation corresponding to interaction between the drug and the target
            new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "interaction" )

            (target_uniprot, drug_chembl) = interaction
            #print(target_uniprot, drug_chembl)

            # Associate the participants of the interaction
            new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[target_uniprot] )
            new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[drug_chembl] )

            if interaction in parser.interaction_to_action:

                action = parser.interaction_to_action[interaction]

                # Associate the interaction with the action of the drug against the target
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier= "CHEMBL_action", value=action, type="unique" ) )

            if interaction in parser.interaction_to_mechanism:

                mechanism = parser.interaction_to_mechanism[interaction]

                # Associate the interaction with the action of the drug against the target
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier= "CHEMBL_mechanism", value=mechanism, type="unique" ) )


            # Insert this external entity relation into database
            self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        print('{} interactions inserted'.format(len(parser.interactions)))

        return


    def create_drug_external_entity(self, parser, drug_chembl):
        """
        Create an external entity of a drug
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "drug" )

        # Annotate it as CHEMBL
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "CHEMBL", value=drug_chembl, type="unique") )

        if drug_chembl in parser.drug_to_atcs:
            for atc in parser.drug_to_atcs[drug_chembl]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "ATC", value=atc, type="cross-reference") )

        if drug_chembl in parser.drug_to_smiles:
            smiles = parser.drug_to_smiles[drug_chembl]
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "SMILES", value=smiles, type="cross-reference") )

        if drug_chembl in parser.drug_to_type:
            drug_type = parser.drug_to_type[drug_chembl]
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "CHEMBL_drug_type", value=drug_type, type="unique") )

        if drug_chembl in parser.drug_to_meshid:
            for mesh_id in parser.drug_to_meshid[drug_chembl]:
                mesh_heading = parser.meshid_to_meshheading[mesh_id]
                # new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "MESH_ID", value=mesh_id, type="cross-reference") )
                # new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "MESH_heading", value=mesh_heading, type="cross-reference") )
                new_external_entity.add_attribute( ExternalEntityRelationAttribute( attribute_identifier= "CHEMBL_indication", 
                                                                                             value=mesh_id, 
                                                                                             type="unique",
                                                                                             additional_fields = {"MESH_heading": mesh_heading} ) )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[drug_chembl] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_protein_external_entity(self, parser, target_uniprot):
        """
        Create an external entity of a protein
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        # Annotate it as uniprot accession
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotAccession", value=target_uniprot, type="cross-reference") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[target_uniprot] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return



class ChEMBL(object):

    def __init__(self, drug_file, target_file, drugtarget_file, indication_file):

        self.drug_file = drug_file
        self.target_file = target_file
        self.drugtarget_file = drugtarget_file
        self.indication_file = indication_file

        self.drugs = set()
        self.drug_to_phase = {}
        self.drug_to_atcs = {}
        self.drug_to_type = {}
        self.drug_to_smiles = {}
        self.drug_to_meshid = {}
        self.meshid_to_meshheading = {}
        self.targets = set()
        self.chembl_to_uniprots = {}
        self.uniprot_to_organism = {}
        self.organisms = set()
        self.chembl_to_type = {}
        self.interactions = set()
        self.interaction_to_action = {}
        self.interaction_to_mechanism = {}

        return

    def parse(self):

        import csv


        print("\n.....PARSING DRUG FILE.....\n")

        with open(self.drug_file, 'r') as drug_fd:

            first_line = drug_fd.readline()
            fields_dict = self.obtain_header_fields(first_line, separator='\t')
            # PARENT_MOLREGNO   CHEMBL_ID   SYNONYMS    DEVELOPMENT_PHASE   RESEARCH_CODES  APPLICANTS  USAN_STEM   USAN_STEM_DEFINITION    USAN_STEM_SUBSTEM   USAN_YEAR   FIRST_APPROVAL  ATC_CODE    ATC_CODE_DESCRIPTION    INDICATION_CLASS    SC_PATENT   DRUG_TYPE   RULE_OF_FIVE    FIRST_IN_CLASS  CHIRALITY   PRODRUG ORAL    PARENTERAL  TOPICAL BLACK_BOX   AVAILABILITY_TYPE   WITHDRAWN_YEAR  WITHDRAWN_COUNTRY   WITHDRAWN_REASON    WITHDRAWN_CLASS CANONICAL_SMILES

            csvreader = csv.reader(drug_fd, delimiter='\t')

            for fields in csvreader:

                chembl_id = fields[ fields_dict['chembl_id'] ].upper()
                phase = int(fields[ fields_dict['development_phase'] ])
                atcs = fields[ fields_dict['atc_code'] ].upper()
                drug_type = fields[ fields_dict['drug_type'] ].lower()
                smiles = fields[ fields_dict['canonical_smiles'] ].upper()

                #print(chembl_id, phase, drug_type, atcs)
                self.drugs.add(chembl_id)

                if atcs != '':
                    atcs = atcs.split('; ')
                    for atc in atcs:
                        self.drug_to_atcs.setdefault(chembl_id, set())
                        self.drug_to_atcs[chembl_id].add(atc)

                if phase != '':
                    self.drug_to_phase[chembl_id] = phase

                if drug_type != '':
                    self.drug_to_type[chembl_id] = drug_type

                if smiles != '':
                    self.drug_to_smiles[chembl_id] = smiles



        print("\n.....PARSING INDICATION FILE.....\n")

        with open(self.indication_file,'r') as indication_fd:

            first_line = indication_fd.readline()
            fields_dict = self.obtain_header_fields(first_line, separator='\t')
            # MOLECULE_CHEMBL_ID    MOLECULE_NAME   MOLECULE_TYPE   FIRST_APPROVAL  MESH_ID MESH_HEADING    EFO_ID  EFO_NAME    MAX_PHASE_FOR_IND   USAN_YEAR   REFS

            csvreader = csv.reader(indication_fd, delimiter='\t')

            for fields in csvreader:

                chembl_id = fields[ fields_dict['molecule_chembl_id'] ].upper()
                drug_name = fields[ fields_dict['molecule_name'] ].lower()
                mesh_id = fields[ fields_dict['mesh_id'] ].upper()
                mesh_heading = fields[ fields_dict['mesh_heading'] ].lower()

                if chembl_id not in self.drugs:
                    continue

                if mesh_id != '' and mesh_heading != '':
                    self.drug_to_meshid.setdefault(chembl_id, set())
                    self.drug_to_meshid[chembl_id].add(mesh_id)
                    self.meshid_to_meshheading[mesh_id] = mesh_heading



        print("\n.....PARSING TARGET FILE.....\n")

        with open(self.target_file,'r') as target_fd:

            first_line = target_fd.readline()
            fields_dict = self.obtain_header_fields(first_line, separator='\t')
            # CHEMBL_ID TID PREF_NAME   PROTEIN_ACCESSION   TARGET_TYPE ORGANISM    COMPOUNDS   ENDPOINTS

            csvreader = csv.reader(target_fd, delimiter='\t')

            for fields in csvreader:

                chembl_id = fields[ fields_dict['chembl_id'] ].upper()
                raw_uniprots = fields[ fields_dict['protein_accession'] ].upper()
                target_type = fields[ fields_dict['target_type'] ].lower()
                organism = fields[ fields_dict['organism'] ].lower()
                uniprot_regex = re.compile("([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2})")

                # Skip the targets without uniprot
                if raw_uniprots == '':
                    print('Skipping target {} because it does not have uniprot!'.format(chembl_id))
                    continue
                else:
                    uniprots = set()
                    raw_uniprots = raw_uniprots.split(', ')
                    for raw_uniprot in raw_uniprots:
                        if raw_uniprot == '':
                            continue
                        # Check uniprot with regex because there are ENSMBL codes mixed
                        m = uniprot_regex.match(raw_uniprot)
                        if m:
                            uniprot = m.group(1)
                            uniprots.add(uniprot)

                # Skip if there are no valid uniprots
                if len(uniprots) == 0:
                    print('Skipping target {} because it does not have valid uniprots!'.format(chembl_id))
                    continue

                # Skip the targets without organism
                if organism == '':
                    continue

                #print(chembl_id, uniprots, target_type, organism)

                if chembl_id in self.chembl_to_type:
                    print('CHEMBL {} registered with multiple types.'.format(chembl_id))
                    sys.exit(10)
                self.chembl_to_type[chembl_id] = target_type

                if chembl_id in self.chembl_to_uniprots:
                    print('Target {} registered with multiple uniprots.'.format(chembl_id))
                    sys.exit(10)
                for uniprot in uniprots:

                    self.chembl_to_uniprots.setdefault(chembl_id, set())
                    self.chembl_to_uniprots[chembl_id].add(uniprot)
                    self.targets.add(uniprot)

                    if uniprot in self.uniprot_to_organism:
                        print('Target {} registered with multiple organisms.'.format(uniprot))
                        #sys.exit(10)
                    self.uniprot_to_organism[uniprot] = organism
                    self.organisms.add(organism)


                    print(chembl_id, uniprot, organism, target_type)



        print("\n.....PARSING DRUG TARGET FILE.....\n")

        with open(self.drugtarget_file,'r') as drugtarget_fd:

            first_line = drugtarget_fd.readline()
            fields_dict = self.obtain_header_fields(first_line, separator='\t')
            # MOLECULE_CHEMBL_ID    MOLECULE_NAME   MOLECULE_TYPE   FIRST_APPROVAL  ATC_CODE    ATC_CODE_DESCRIPTION    USAN_STEM   MECHANISM_OF_ACTION MECHANISM_COMMENT   SELECTIVITY_COMMENT TARGET_CHEMBL_ID    TARGET_NAME ACTION_TYPE ORGANISM    TARGET_TYPE SITE_NAME   BINDING_SITE_COMMENT    MECHANISM_REFS  CANONICAL_SMILES

            csvreader = csv.reader(drugtarget_fd, delimiter='\t')

            for fields in csvreader:

                drug_chembl_id = fields[ fields_dict['molecule_chembl_id'] ].upper()
                drug_name = fields[ fields_dict['molecule_name'] ].lower()
                mechanism_of_action = fields[ fields_dict['mechanism_of_action'] ].lower()
                target_chembl_id = fields[ fields_dict['target_chembl_id'] ].upper()
                target_name = fields[ fields_dict['target_name'] ].lower()
                action_type = fields[ fields_dict['action_type'] ].lower()
                organism = fields[ fields_dict['organism'] ].lower()

                if drug_chembl_id == '' or target_chembl_id == '':
                    continue

                if drug_chembl_id not in self.drugs or target_chembl_id not in self.chembl_to_uniprots:
                    print('Skipping drug {} and target {}!'.format(drug_chembl_id, target_chembl_id))
                    continue

                for uniprot in self.chembl_to_uniprots[target_chembl_id]:

                    interaction = (uniprot, drug_chembl_id)
                    self.interactions.add(interaction)

                    #print(drug_chembl_id, uniprot, target_name, action_type, mechanism_of_action)

                    if action_type != '':
                        self.interaction_to_action[interaction] = action_type
                        #print(action_type)
                    if mechanism_of_action != '':
                        self.interaction_to_mechanism[interaction] = mechanism_of_action

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

