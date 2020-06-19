from bianaParser import *
import csv
          
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

        # Add a different type of relation
        self.biana_access.add_valid_external_entity_relation_type( type = "drug_indication_association" )

        # Add new external entity attributes
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
                                                                    data_type = "text",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "MESH_ID",
                                                                    data_type = "varchar(30)",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "MESH_name",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE descriptive attribute")

        data_type_dict = {"fields": [ ("value","double"), ("activity_type","varchar(30)",False), ("relation","varchar(30)",False), ("unit","varchar(30)",False) ], "indices": ("value",)}
        self.biana_access.add_valid_external_entity_attribute_type( name = "CHEMBL_drug_target_activity",
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

        drug_file = None
        target_file = None
        drugtarget_file = None
        indication_file = None
        activity_file = None
        input_files = [f for f in os.listdir(self.input_path) if os.path.isfile(os.path.join(self.input_path, f))]
        for input_file in input_files:
            if input_file.endswith('drugs.tsv'):
                drug_file = os.path.join(self.input_path, input_file)
            elif input_file.endswith('targets.tsv'):
                target_file = os.path.join(self.input_path, input_file)
            elif input_file.endswith('mechanisms.tsv'):
                drugtarget_file = os.path.join(self.input_path, input_file)
            elif input_file.endswith('indications.tsv'):
                indication_file = os.path.join(self.input_path, input_file)
            elif input_file.endswith('activities.tsv'):
                activity_file = os.path.join(self.input_path, input_file)

        if not drug_file:
            print('The drugs file is missing!')
            sys.exit(10)
        if not target_file:
            print('The targets file is missing!')
            sys.exit(10)
        if not indication_file:
            print('The indications file is missing!')
            sys.exit(10)
        if not drugtarget_file:
            print('The drug-target interactions file is missing!')
            sys.exit(10)


        ########################
        #### PARSE DATABASE ####
        ########################

        parser = ChEMBL(drug_file, target_file, drugtarget_file, indication_file, activity_file)
        parser.parse_drug_file()
        parser.parse_indication_file()
        parser.parse_target_file()
        parser.parse_drug_target_file(drugs_to_parse=parser.drugs, targets_to_parse=parser.targets)
        parser.parse_activity_file(interactions_to_parse=parser.interactions)


        #########################################
        #### INSERT PARSED DATABASE IN BIANA ####
        #########################################

        self.external_entity_ids_dict = {}

        #--------------#
        # INSERT DRUGS #
        #--------------#

        print("\n.....INSERTING THE DRUGS IN THE DATABASE.....\n")

        for drug_chembl in parser.drugs:

            # Create an external entity corresponding to the drug in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(drug_chembl):

                self.create_drug_external_entity(parser, drug_chembl)
                #print(drug_chembl)

        print('{} drugs inserted'.format(len(parser.drugs)))


        #----------------#
        # INSERT TARGETS #
        #----------------#

        print("\n.....INSERTING THE PROTEINS IN THE DATABASE.....\n")
        uniprots_inserted = set()

        for target_chembl in parser.targets:

            if target_chembl in parser.target_chembl_to_uniprots:

                for target_uniprot in parser.target_chembl_to_uniprots[target_chembl]:

                    # Create an external entity corresponding to the target in the database (if it is not already created)
                    if not self.external_entity_ids_dict.has_key(target_uniprot):

                        self.create_protein_external_entity(parser, target_uniprot)
                        uniprots_inserted.add(target_uniprot)
                        #print(target_uniprot)

        print('{} proteins inserted'.format(len(uniprots_inserted)))


        #-----------------#
        # INSERT DISEASES #
        #-----------------#

        print("\n.....INSERTING THE DISEASES IN THE DATABASE.....\n")

        for disease_MESH in parser.diseases:

            # Create an external entity corresponding to the disease in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(disease_MESH):

                self.create_disease_external_entity(parser, disease_MESH)
                #print(disease_MESH)

        print('{} diseases inserted'.format(len(parser.diseases)))


        #---------------------#
        # INSERT INTERACTIONS #
        #---------------------#

        print("\n.....INSERTING THE INTERACTIONS IN THE DATABASE.....\n")
        interactions_inserted = set()

        for interaction in parser.interactions:

            (target_chembl, drug_chembl) = interaction
            #print(target_chembl, drug_chembl)

            if target_chembl in parser.target_chembl_to_uniprots:

                for target_uniprot in parser.target_chembl_to_uniprots[target_chembl]:

                    uniprot_drug_interaction = (target_uniprot, drug_chembl)

                    # Create an external entity corresponding to the interaction in the database (if it is not already created)
                    if not self.external_entity_ids_dict.has_key(uniprot_drug_interaction):

                        # Create an external entity relation corresponding to interaction between the drug and the target
                        new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "interaction" )

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

                        if interaction in parser.interaction_to_activity_values:

                            for activity_values in parser.interaction_to_activity_values[interaction]:

                                [standard_type, standard_relation, standard_value, standard_units] = activity_values

                                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier= "CHEMBL_drug_target_activity", 
                                                                                                             value=standard_value, 
                                                                                                             type="unique",
                                                                                                             additional_fields = {"activity_type": standard_type, "relation":standard_relation, "unit":standard_units} ) )


                        # Insert this external entity relation into database
                        self.external_entity_ids_dict[uniprot_drug_interaction] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

                        interactions_inserted.add(uniprot_drug_interaction)

        print('{} interactions inserted'.format(len(interactions_inserted)))


        #--------------------#
        # INSERT INDICATIONS #
        #--------------------#

        print("\n.....INSERTING THE INDICATIONS IN THE DATABASE.....\n")

        for indication in parser.indications:

            # Create an external entity corresponding to the interaction in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(indication):

                (mesh_id, drug_chembl) = indication
                #print(mesh_id, drug_chembl)

                # Create an external entity relation corresponding to interaction between the drug and the target
                new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "drug_indication_association" )

                # Associate the participants of the interaction
                new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[mesh_id] )
                new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[drug_chembl] )

                # Insert this external entity relation into database
                self.external_entity_ids_dict[indication] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        print('{} indications inserted'.format(len(parser.indications)))

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

        # Annotate it's taxonomy identifiers (there can be multiple ones!)
        if target_uniprot in parser.uniprot_to_taxids:
            for taxid in parser.uniprot_to_taxids[target_uniprot]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "TaxID", value=taxid, type="cross-reference") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[target_uniprot] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_disease_external_entity(self, parser, disease_MESH):
        """
        Create an external entity of a disease and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "disease" )

        # Annotate its disease MESH
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "MESH_ID", value=disease_MESH.upper(), type="cross-reference") )

        # Annotate its disease MESH
        if disease_MESH in parser.meshid_to_meshheading:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "MESH_name", value=parser.meshid_to_meshheading[disease_MESH].lower(), type="cross-reference") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[disease_MESH] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return



class ChEMBL(object):

    def __init__(self, drug_file, target_file, drugtarget_file, indication_file, activity_file=None):

        self.drug_file = drug_file
        self.target_file = target_file
        self.drugtarget_file = drugtarget_file
        self.indication_file = indication_file
        self.activity_file = activity_file

        self.drugs = set()
        self.drug_to_phase = {}
        self.drug_to_atcs = {}
        self.drug_to_type = {}
        self.drug_to_smiles = {}
        self.drug_to_meshid = {}
        self.meshid_to_meshheading = {}
        self.drug_to_efoids = {}
        self.drug_to_efonames = {}
        self.targets = set()
        self.target_chembl_to_type = {}
        self.target_chembl_to_uniprots = {}
        self.uniprot_to_taxids = {}
        self.organisms = set()
        self.interactions = set()
        self.interaction_to_action = {}
        self.interaction_to_mechanism = {}
        self.diseases = set()
        self.indications = set()
        self.interaction_to_activity_values = {}

        return


    def parse_drug_file(self):
        """
        Parsing of the drug file of ChEMBL
        """

        print("\n.....PARSING DRUG FILE.....\n")

        num_line = 0

        with open(self.drug_file, 'r') as drug_fd:

            csvreader = csv.reader(drug_fd, delimiter='\t')

            for fields in csvreader:

                num_line += 1

                # Obtain a dictionary: "field_name" => "position"
                if num_line == 1:
                    # PARENT_MOLREGNO   CHEMBL_ID   SYNONYMS    DEVELOPMENT_PHASE   RESEARCH_CODES  APPLICANTS  USAN_STEM   USAN_STEM_DEFINITION    USAN_STEM_SUBSTEM   USAN_YEAR   FIRST_APPROVAL  ATC_CODE    ATC_CODE_DESCRIPTION    INDICATION_CLASS    SC_PATENT   DRUG_TYPE   RULE_OF_FIVE    FIRST_IN_CLASS  CHIRALITY   PRODRUG ORAL    PARENTERAL  TOPICAL BLACK_BOX   AVAILABILITY_TYPE   WITHDRAWN_YEAR  WITHDRAWN_COUNTRY   WITHDRAWN_REASON    WITHDRAWN_CLASS CANONICAL_SMILES
                    # "Parent Molecule"	"Synonyms"	"Research Codes"	"Phase"	"Applicants"	"USAN Stem"	"USAN Year"	"USAN Stem Definition"	"USAN Stem Substem"	"First Approval"	"ATC Codes"	"Level 4 ATC Codes"	"Level 3 ATC Codes"	"Level 2 ATC Codes"	"Level 1 ATC Codes"	"Indication Class"	"Patent"	"Drug Type"	"Passes Rule of Five"	"First In Class"	"Chirality"	"Prodrug"	"Oral"	"Parenteral"	"Topical"	"Black Box"	"Availability Type"	"Withdrawn Year"	"Withdrawn Reason"	"Withdrawn Country"	"Withdrawn Class"	"Smiles"
                    fields_dict = self.obtain_header_fields( '\t'.join(fields), separator='\t')
                    continue

                chembl_id = fields[ fields_dict['Parent Molecule'] ].upper()
                phase = int(fields[ fields_dict['Phase'] ]) # Number (i.e. "0", "4")
                atcs = fields[ fields_dict['ATC Codes'] ].upper() # They can be multiple (i.e. "R07AB53 | R07AB03")
                drug_type = fields[ fields_dict['Drug Type'] ].lower() # set(['7:natural product-derived', '6:antibody', '9:inorganic', '8:cell-based', '-1:unknown', '10:polymer', '5:oligopeptide', '2:enzyme', '3:oligosaccharide', '1:synthetic small molecule', '4:oligonucleotide'])
                smiles = fields[ fields_dict['Smiles'] ].upper()

                #print(chembl_id, phase, drug_type, atcs)
                self.drugs.add(chembl_id)

                if atcs != '':
                    atcs = atcs.split(' | ')
                    for atc in atcs:
                        self.drug_to_atcs.setdefault(chembl_id, set())
                        self.drug_to_atcs[chembl_id].add(atc)

                if phase != '':
                    self.drug_to_phase[chembl_id] = phase

                if drug_type != '':
                    if ':' in drug_type:
                        drug_type = drug_type.split(':')[1]
                    self.drug_to_type[chembl_id] = drug_type

                if smiles != '':
                    self.drug_to_smiles[chembl_id] = smiles

        return


    def parse_indication_file(self, chembl_ids_to_parse=None):
        """
        Parsing of the indication file of ChEMBL
        """

        print("\n.....PARSING INDICATION FILE.....\n")

        num_line = 0

        with open(self.indication_file,'r') as indication_fd:

            csvreader = csv.reader(indication_fd, delimiter='\t')

            for fields in csvreader:

                num_line += 1

                if num_line == 1:
                    # MOLECULE_CHEMBL_ID    MOLECULE_NAME   MOLECULE_TYPE   FIRST_APPROVAL  MESH_ID MESH_HEADING    EFO_ID  EFO_NAME    MAX_PHASE_FOR_IND   USAN_YEAR   REFS
                    # "Parent Molecule ChEMBL ID"	"Parent Molecule Name"	"Parent Molecule Type"	"Max Phase for Indication"	"First Approval"	"MESH ID"	"MESH Heading"	"EFO IDs"	"EFO Terms"	"References"	"Synonyms"	"USAN Stem"	"USAN Year"
                    fields_dict = self.obtain_header_fields( '\t'.join(fields), separator='\t')
                    continue

                chembl_id = fields[ fields_dict['Parent Molecule ChEMBL ID'] ].upper()
                drug_name = fields[ fields_dict['Parent Molecule Name'] ].lower()
                mesh_id = fields[ fields_dict['MESH ID'] ].upper() # There is only one
                mesh_heading = fields[ fields_dict['MESH Heading'] ].lower() # There is only one
                efo_ids = fields[ fields_dict['EFO IDs'] ].upper() # There could be multiple ones (e.g. "EFO:0005854|EFO:0003956")
                efo_terms = fields[ fields_dict['EFO Terms'] ].lower() # There could be multiple ones (e.g. "allergic rhinitis|seasonal allergic rhinitis")

                if chembl_ids_to_parse:
                    if chembl_id not in chembl_ids_to_parse:
                        continue

                self.drugs.add(chembl_id)

                if mesh_id != '' and mesh_heading != '':
                    self.diseases.add(mesh_id)
                    self.drug_to_meshid.setdefault(chembl_id, set()).add(mesh_id)
                    self.meshid_to_meshheading[mesh_id] = mesh_heading
                    indication = (mesh_id, chembl_id)
                    self.indications.add(indication)
                # EFO IDs not added because the mapping MESH to EFO is already included in DisGeNET.
                # I leave it commented so that if we need them, we can easily add them
                # if efo_ids != '' and efo_terms != '':
                #     for efo_id in efo_ids.split('|'):
                #         self.drug_to_efoids.setdefault(chembl_id, set()).add(efo_id)
                #     for efo_term in efo_terms.split('|'):
                #         self.drug_to_efonames.setdefault(chembl_id, set()).add(efo_term)

        return


    def parse_target_file(self):
        """
        Parsing of the target file of ChEMBL
        """

        print("\n.....PARSING TARGET FILE.....\n")

        num_line = 0

        with open(self.target_file,'r') as target_fd:

            csvreader = csv.reader(target_fd, delimiter='\t')

            for fields in csvreader:

                num_line += 1

                if num_line == 1:
                    # CHEMBL_ID TID PREF_NAME   PROTEIN_ACCESSION   TARGET_TYPE ORGANISM    COMPOUNDS   ENDPOINTS
                    #"ChEMBL ID"	"Name"	"UniProt Accessions"	"Type"	"Organism"	"Compounds"	"Activities"	"Tax ID"	"Species Group Flag"
                    fields_dict = self.obtain_header_fields( '\t'.join(fields), separator='\t')
                    continue


                chembl_id = fields[ fields_dict['ChEMBL ID'] ].upper()
                raw_uniprots = fields[ fields_dict['UniProt Accessions'] ].upper() # They can be multiple uniprots: "Q15125|Q9UBM7"
                target_type = fields[ fields_dict['Type'] ].lower()
                organism = fields[ fields_dict['Organism'] ].lower()
                taxid = fields[ fields_dict['Tax ID'] ]
                uniprot_regex = re.compile("([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2})")

                # Skip the targets without uniprot
                if raw_uniprots == '':
                    #print('Skipping target {} because it does not have uniprot!'.format(chembl_id))
                    continue
                else:
                    uniprots = set()
                    raw_uniprots = raw_uniprots.split('|')
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
                    #print('Skipping target {} because it does not have valid uniprots!'.format(chembl_id))
                    continue

                # Skip the targets without organism
                if taxid == '':
                    print('Skipping target {} because it does not have Tax ID!'.format(chembl_id))
                    continue
                self.organisms.add(taxid)

                #print(chembl_id, uniprots, target_type, organism)

                if chembl_id in self.targets:
                    print('CHEMBL {} registered with multiple types.'.format(chembl_id))
                    sys.exit(10)

                self.targets.add(chembl_id)
                self.target_chembl_to_type[chembl_id] = target_type

                for uniprot in uniprots:
                    self.target_chembl_to_uniprots.setdefault(chembl_id, set()).add(uniprot)
                    self.uniprot_to_taxids.setdefault(uniprot, set()).add(taxid)
                    # if len(self.uniprot_to_taxids[chembl_id]) > 1:
                    #     print('Target {} registered with multiple organisms.'.format(uniprot))
                    #     pass


                #print(chembl_id, target_type, uniprots, taxid)

        return


    def parse_drug_target_file(self, drugs_to_parse=None, targets_to_parse=None):
        """
        Parsing of the drug target file (or mechanisms file) of ChEMBL
        If you provide a list of chembl IDs in drugs_to_parse, it will parse only the targets of these drugs.
        Same with targets_to_parse.
        """

        print("\n.....PARSING DRUG TARGET FILE.....\n")

        num_line = 0

        with open(self.drugtarget_file,'r') as drugtarget_fd:

            csvreader = csv.reader(drugtarget_fd, delimiter='\t')

            for fields in csvreader:

                num_line += 1

                if num_line == 1:
                    # MOLECULE_CHEMBL_ID    MOLECULE_NAME   MOLECULE_TYPE   FIRST_APPROVAL  ATC_CODE    ATC_CODE_DESCRIPTION    USAN_STEM   MECHANISM_OF_ACTION MECHANISM_COMMENT   SELECTIVITY_COMMENT TARGET_CHEMBL_ID    TARGET_NAME ACTION_TYPE ORGANISM    TARGET_TYPE SITE_NAME   BINDING_SITE_COMMENT    MECHANISM_REFS  CANONICAL_SMILES
                    # "Parent Molecule ChEMBL ID"	"Parent Molecule Name"	"Parent Molecule Type"	"Max Phase"	"First Approval"	"USAN Stem"	"Smiles"	"Mechanism of Action"	"Mechanism Comment"	"Selectivity Comment"	"Target ChEMBL ID"	"Target Name"	"Action Type"	"Target Type"	"Target Organism"	"Binding Site Name"	"Binding Site Comment"	"References"	"Synonyms"	"ATC Codes"	"Level 4 ATC Codes"	"Level 3 ATC Codes"	"Level 2 ATC Codes"	"Level 1 ATC Codes"	"Parent Molecule ChEMBL ID"
                    fields_dict = self.obtain_header_fields( '\t'.join(fields), separator='\t')
                    continue

                drug_chembl_id = fields[ fields_dict['Parent Molecule ChEMBL ID'] ].upper()
                drug_name = fields[ fields_dict['Parent Molecule Name'] ].lower()
                mechanism_of_action = fields[ fields_dict['Mechanism of Action'] ].lower()
                target_chembl_id = fields[ fields_dict['Target ChEMBL ID'] ].upper()
                target_name = fields[ fields_dict['Target Name'] ].lower()
                action_type = fields[ fields_dict['Action Type'] ].lower()
                organism = fields[ fields_dict['Target Organism'] ].lower()

                if drug_chembl_id == '' or target_chembl_id == '':
                    continue

                if drugs_to_parse:
                    # Skip drugs that are not in drugs_to_parse list
                    if drug_chembl_id not in drugs_to_parse:
                        #print('Skipping drug {}!'.format(drug_chembl_id))
                        continue

                if targets_to_parse:
                    # Skip targets that are not in targets_to_parse list
                    if target_chembl_id not in targets_to_parse:
                        #print('Skipping target {}!'.format(target_chembl_id))
                        continue

                # Include interaction
                interaction = (target_chembl_id, drug_chembl_id)
                self.interactions.add(interaction)

                # Include attributes of the interaction
                if action_type != '':
                    self.interaction_to_action[interaction] = action_type

                if mechanism_of_action != '':
                    self.interaction_to_mechanism[interaction] = mechanism_of_action

                #print(drug_chembl_id, target_chembl_id, action_type, mechanism_of_action)

        return


    def parse_activity_file(self, interactions_to_parse=None):
        """
        Parsing of the activity of ChEMBL
        If you provide a list of chembl IDs in drugs_to_parse, it will parse only the targets of these drugs.
        Same with targets_to_parse.
        """

        print("\n.....PARSING ACTIVITY FILE.....\n")

        num_line = 0

        with open(self.activity_file,'r') as activity_fd:

            csvreader = csv.reader(activity_fd, delimiter='\t')

            for fields in csvreader:

                num_line += 1

                if num_line == 1:
                    # "Molecule ChEMBL ID"	"Molecule Name"	"Molecule Max Phase"	"Molecular Weight"	"#RO5 Violations"	"AlogP"	"Compound Key"	"Smiles"	"Standard Type"	"Standard Relation"	"Standard Value"	"Standard Units"	"pChEMBL Value"	"Data Validity Comment"	"Comment"	"Uo Units"	"Ligand Efficiency BEI"	"Ligand Efficiency LE"	"Ligand Efficiency LLE"	"Ligand Efficiency SEI"	"Potential Duplicate"	"Assay ChEMBL ID"	"Assay Description"	"Assay Type"	"BAO Format ID"	"BAO Label"	"Assay Organism"	"Assay Tissue ChEMBL ID"	"Assay Tissue Name"	"Assay Cell Type"	"Assay Subcellular Fraction"	"Target ChEMBL ID"	"Target Name"	"Target Organism"	"Target Type"	"Document ChEMBL ID"	"Source ID"	"Source Description"	"Document Journal"	"Document Year"	"Cell ChEMBL ID"
                    fields_dict = self.obtain_header_fields( '\t'.join(fields), separator='\t')
                    continue

                drug_chembl_id = fields[ fields_dict['Molecule ChEMBL ID'] ].upper()
                drug_name = fields[ fields_dict['Molecule Name'] ].lower()
                target_chembl_id = fields[ fields_dict['Target ChEMBL ID'] ].upper()
                target_name = fields[ fields_dict['Target Name'] ].lower()
                organism = fields[ fields_dict['Target Organism'] ].lower()
                standard_type = fields[ fields_dict['Standard Type'] ]
                standard_relation = fields[ fields_dict['Standard Relation'] ]
                standard_value = fields[ fields_dict['Standard Value'] ]
                standard_units = fields[ fields_dict['Standard Units'] ]

                if drug_chembl_id == '' or target_chembl_id == '':
                    continue

                # Skip drug-target relations with missing activity attributes
                if standard_type == '' or standard_relation == '' or standard_value == '' or standard_units == '':
                    continue
                
                interaction = (target_chembl_id, drug_chembl_id)

                if interactions_to_parse:
                    # Skip drugs that are not in drugs_to_parse list
                    if interaction not in interactions_to_parse:
                        #print('Skipping drug {}!'.format(drug_chembl_id))
                        continue

                # Include activity values
                self.interaction_to_activity_values.setdefault(interaction, []).append([standard_type, standard_relation, float(standard_value), standard_units])
                #print(target_chembl_id, drug_chembl_id, standard_type, standard_relation, float(standard_value), standard_units)

        #print(self.interaction_to_activity_values)

        return


    def obtain_header_fields(self, first_line, separator='\t'):
        """ 
        Obtain a dictionary: "field_name" => "position" 
        """
        fields_dict = {}

        header_fields = first_line.strip().split(separator)
        for x in xrange(0, len(header_fields)):
            fields_dict[header_fields[x]] = x

        return fields_dict


if __name__ == "__main__":
    main()

