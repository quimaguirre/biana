from bianaParser import *
import csv

class TTDParser(BianaParser):
    """
    MyData Parser Class 

    Parses data from the TTD
    """

    name = "TTD"
    description = "This file implements a program that fills up tables in BIANA database from data in the TTD"
    external_entity_definition = "An external entity represents a drug or a protein"
    external_entity_relations = "An external relation represents a drug combination or an interaction between drugs or drug-target"

    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "TTD parser",  
                             default_script_name = "TTDParser.py",
                             default_script_description = TTDParser.description,     
                             additional_compulsory_arguments = [])
        self.default_eE_attribute = "TTD_drugID"

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
        self.biana_access.add_valid_external_entity_attribute_type( name = "TTD_drugID",
                                                                    data_type = "varchar(30)",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "TTD_targetID",
                                                                    data_type = "varchar(30)",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "TTD_target_type",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "PubChemSubstance",
                                                                    data_type = "int unsigned",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "ICD11_ID",
                                                                    data_type = "varchar(30)",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "ICD11_name",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE descriptive attribute")

        self.biana_access.refresh_database_information()



        ###########################
        #### CHECK INPUT FILES ####
        ###########################

        # Check that all the files exist
        if os.path.isdir(self.input_file):
            self.input_path = self.input_file
        else:
            raise ValueError("You must specify a path instead of a file")

        required_files = [  'P1-01-TTD_target_download.txt', 'P1-02-TTD_drug_download.txt', 'P1-03-TTD_crossmatching.txt', 'P1-04-Drug_synonyms.txt', 
                            'P1-05-Drug_disease.txt', 'P1-06-Target_disease.txt', 'P1-07-Drug-TargetMapping.txt', 'P1-08-Biomarker_disease.txt' ]

        """
        TTD FILES GUIDE:
        ---------------
        P1: TTD Database Downlaods
            P1-01-TTD_target_download.txt       =>  Download TTD targets information in raw format              => PARSE
            P1-02-TTD_drug_download.txt         =>  Download TTD drug information in raw format                 => NO
            P1-03-TTD_crossmatching.txt         =>  Cross-matching ID between TTD drugs and public databases    => PARSE
            P1-04-Drug_synonyms.txt             =>  Synonyms of drugs and small molecules in TTD                => NO
            P1-05-Drug_disease.txt              =>  Drug to disease mapping with ICD identifiers                => PARSE
            P1-06-Target_disease.txt            =>  Target to disease mapping with ICD identifiers              => NO
            P1-07-Drug-TargetMapping.txt        =>  Target to drug mapping with mode of action                  => NO
            P1-08-Biomarker_disease.txt         =>  Biomarker to disease mapping with ICD identifiers           => NO
        
        P2: Target Information Downloads
            P2-01-TTD_uniprot_all.txt           =>  Download Uniprot IDs for all targets                        => NO
        
        P3: Drug Structure Downloads
            P3-01-All.sdf                       =>  Download structure data for all drugs in SDF format         => NO
        
        P4: Pathway Information Downloads
            P4-01-Target-KEGGpathway_all.txt    =>  Download KEGG pathway data for all targets                  => NO
            P4-06-Target-wikipathway_all.txt    =>  Download Wiki pathway data for all targets                  => NO
        
        P5: Drug Combinations Downloads
            P5-01-Table1                        =>  Pharmacodynamically synergistic drug combinations due to anti-counteractive actions     => Manually curate the file
            P5-02-Table2                        =>  Pharmacodynamically synergistic drug combinations due to complementary actions          => Manually curate the file
            P5-02-Table3                        =>  Pharmacodynamically synergistic drug combinations due to facilitating actions           => Manually curate the file
            P5-04-Table4                        =>  Pharmacodynamically additive drug combinations                                          => Manually curate the file
            P5-05-Table5                        =>  Pharmacodynamically antagonistic drug combinations                                      => Manually curate the file
            P5-06-Table6                        =>  Pharmacokinetically potentiative drug combinations                                      => NO
            P5-07-Table7                        =>  Pharmacokinetically reductive drug combinations                                         => NO
        """


        for current_file in required_files:
            if os.path.exists( os.path.join(self.input_path, current_file) ) is False:
                raise ValueError('File {} is missing in {}'.format(current_file, self.input_path))


        ########################
        #### PARSE DATABASE ####
        ########################

        parser = TTD(self.input_path)
        parser.parse_target_info()
        parser.parse_crossmatching()
        parser.parse_drug_synonyms()
        parser.parse_drug_disease()


        #########################################
        #### INSERT PARSED DATABASE IN BIANA ####
        #########################################

        self.external_entity_ids_dict = {}

        #--------------#
        # INSERT DRUGS #
        #--------------#

        print("\n.....INSERTING THE DRUGS IN THE DATABASE.....\n")

        for drug_id in parser.drug_ids:

            # Create an external entity corresponding to the drug in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(drug_id):

                self.create_drug_external_entity(parser, drug_id)
                #print(drug_id)

        print('{} drugs inserted'.format(len(parser.drug_ids)))


        #----------------#
        # INSERT TARGETS #
        #----------------#

        print("\n.....INSERTING THE PROTEINS IN THE DATABASE.....\n")

        for target_id in parser.target_ids:

            # Create an external entity corresponding to the target in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(target_id):

                self.create_protein_external_entity(parser, target_id)
                #print(target_id)

        print('{} proteins inserted'.format(len(parser.target_ids)))


        #-----------------#
        # INSERT DISEASES #
        #-----------------#

        print("\n.....INSERTING THE DISEASES IN THE DATABASE.....\n")

        icd11_codes = set()
        icd11_results = parser.drugid_to_icd_to_status.values()
        for result in icd11_results:
            icd_codes = result.keys()
            for icd11 in icd_codes:
                #print(icd11)

                # Create an external entity corresponding to the disease in the database (if it is not already created)
                if not self.external_entity_ids_dict.has_key(icd11):

                    self.create_disease_external_entity(parser, icd11)
                    icd11_codes.add(icd11)
                    #print(icd11)

        print('{} diseases inserted'.format(len(icd11_codes)))


        #---------------------------------#
        # INSERT DRUG-TARGET INTERACTIONS #
        #---------------------------------#

        print("\n.....INSERTING THE DRUG-TARGET INTERACTIONS IN THE DATABASE.....\n")
        interactions_inserted = set()

        for target_id in parser.targetid_to_drugids:

            for drug_id in parser.targetid_to_drugids[target_id]:

                interaction = (target_id, drug_id)

                # Create an external entity corresponding to the interaction in the database (if it is not already created)
                if not self.external_entity_ids_dict.has_key(interaction):

                    # Create an external entity relation corresponding to interaction between the drug and the target
                    new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "interaction" )

                    # Associate the participants of the interaction
                    new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[target_id] )
                    new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[drug_id] )

                    # Insert this external entity relation into database
                    self.external_entity_ids_dict[interaction] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

                    interactions_inserted.add(interaction)

        print('{} interactions inserted'.format(len(interactions_inserted)))


        #--------------------#
        # INSERT INDICATIONS #
        #--------------------#

        indications = set()
        print("\n.....INSERTING THE INDICATIONS IN THE DATABASE.....\n")
        # In this file there are some drug combinations (e.g. D0BA6T-D0U4UQ). I will parse them as drugs.

        for drug_id in parser.drugid_to_icd_to_status:

            for icd11 in parser.drugid_to_icd_to_status[drug_id].keys():

                indication = (drug_id, icd11)

                # Create an external entity corresponding to the interaction in the database (if it is not already created)
                if not self.external_entity_ids_dict.has_key(indication):

                    #print(drug_id, icd11)

                    # Create an external entity relation corresponding to interaction between the drug and the target
                    new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "drug_indication_association" )

                    # Associate the participants of the interaction
                    new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[drug_id] )
                    new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[icd11] )

                    # Insert this external entity relation into database
                    self.external_entity_ids_dict[indication] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )
                    indications.add(indication)

        print('{} indications inserted'.format(len(indications)))

        return



    def create_drug_external_entity(self, parser, drug_id):
        """
        Create an external entity of a drug
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "drug" )

        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "TTD_drugID", value=drug_id, type="unique") )

        if drug_id in parser.drugid_to_drugname:
            drug_name = parser.drugid_to_drugname[drug_id]
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Name", value=drug_name, type="cross-reference") )

        if drug_id in parser.drugid_to_drugsynonyms:
            for synonym in parser.drugid_to_drugsynonyms[drug_id]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Name", value=synonym, type="synonym") )

        if drug_id in parser.drugid_to_chebis:
            for chebi in parser.drugid_to_chebis[drug_id]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "CHEBI", value=chebi, type="cross-reference") )

        if drug_id in parser.drugid_to_chembls:
            for chembl in parser.drugid_to_chembls[drug_id]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "CHEMBL", value=chembl, type="cross-reference") )

        if drug_id in parser.drugid_to_pubchem_cids:
            for pubchem_cid in parser.drugid_to_pubchem_cids[drug_id]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "PubChemCompound", value=pubchem_cid, type="cross-reference") )

        if drug_id in parser.drugid_to_pubchem_sids:
            for pubchem_sid in parser.drugid_to_pubchem_sids[drug_id]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "PubChemSubstance", value=pubchem_sid, type="cross-reference") )

        if drug_id in parser.drugid_to_atcs:
            for atc in parser.drugid_to_atcs[drug_id]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "ATC", value=atc, type="cross-reference") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[drug_id] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_protein_external_entity(self, parser, target_id):
        """
        Create an external entity of a protein
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "TTD_targetID", value=target_id, type="unique") )

        if target_id in parser.targetid_to_uniprot:
            for uniprot_entry in parser.targetid_to_uniprot[target_id]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotEntry", value=uniprot_entry, type="cross-reference") )

        if target_id in parser.targetid_to_genesymbol:
            for genesymbol in parser.targetid_to_genesymbol[target_id]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneSymbol", value=genesymbol, type="cross-reference") )

        if target_id in parser.targetid_to_type:
            for target_type in parser.targetid_to_type[target_id]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "TTD_target_type", value=target_type, type="unique") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[target_id] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_disease_external_entity(self, parser, icd11):
        """
        Create an external entity of a disease and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "disease" )

        # Annotate its disease ICD11
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "ICD11_ID", value=icd11, type="cross-reference") )

        # Annotate its ICD11 name
        if icd11 in parser.icd_to_indication:
            icd11_name = parser.icd_to_indication[icd11]
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "ICD11_name", value=icd11_name, type="cross-reference") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[icd11] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return




class TTD(object):

    def __init__(self, input_path):

        self.input_path = input_path
        self.p1_01_target_info_file = os.path.join(self.input_path, 'P1-01-TTD_target_download.txt')
        self.p1_03_crossmatching_file = os.path.join(self.input_path, 'P1-03-TTD_crossmatching.txt')
        self.p1_04_synonyms_file = os.path.join(self.input_path, 'P1-04-Drug_synonyms.txt')
        self.p1_05_drug_disease_file = os.path.join(self.input_path, 'P1-05-Drug_disease.txt')

        self.target_ids = set()
        self.targetid_to_uniprot = {}
        self.targetid_to_genesymbol = {}
        self.targetid_to_function = {}
        self.targetid_to_type = {}
        self.targetid_to_keggpath = {}
        self.targetid_to_drugids = {}

        self.drug_ids = set()
        self.drugid_to_drugname = {}
        self.drugid_to_drugsynonyms = {}
        self.drugid_to_clinicalstatus = {}
        self.drugid_to_chebis = {}
        self.drugid_to_chembls = {}
        self.drugid_to_pubchem_cids = {}
        self.drugid_to_pubchem_sids = {}
        self.drugid_to_atcs = {}
        self.drugid_to_icd_to_status = {}

        self.icd_to_indication = {}

        return


    def parse_target_info(self):
        """
        Parsing TTD target info
        """

        print("\n.....PARSING TTD TARGET INFO FILE.....\n")

        uniprot_entry_regex = re.compile(r'^(\w{1,5}_\w{1,5})$')

        with open(self.p1_01_target_info_file, 'r') as p1_01_target_info_fd:

            csvreader = csv.reader(p1_01_target_info_fd, delimiter='\t')

            for fields in csvreader:

                if len(fields) >= 3:
                    # FIELD1 = TARGETID / FIELD2 = CATEGORY / FIELD3 = VALUE
                    # T47101	FORMERID	TTDC00024
                    target_id = fields[0].upper()
                    category = fields[1]
                    self.target_ids.add(target_id)
                    if category == 'UNIPROID':
                        target_uniprot_entries = set()
                        raw_uniprot_entries = fields[2].upper().split('; ')
                        for raw_uniprot_entry in raw_uniprot_entries:
                            m = uniprot_entry_regex.match(raw_uniprot_entry)
                            if m:
                                uniprot = m.group(1)
                                target_uniprot_entries.add(uniprot)
                        for target_uniprot_entry in target_uniprot_entries:
                            self.targetid_to_uniprot.setdefault(target_id, set()).add(target_uniprot_entry)
                    elif category == 'TARGNAME':
                        target_name = fields[2].lower()
                    elif category == 'GENENAME':
                        target_genesymbols = fields[2].upper().split('; ')
                        for target_genesymbol in target_genesymbols:
                            self.targetid_to_genesymbol.setdefault(target_id, set()).add(target_genesymbol)
                    elif category == 'TARGTYPE':
                        target_type = fields[2].lower()
                        self.targetid_to_type.setdefault(target_id, set()).add(target_type)
                    elif category == 'FUNCTION':
                        target_function = fields[2]
                        self.targetid_to_function.setdefault(target_id, set()).add(target_function)
                    elif category == 'PDBSTRUC':
                        target_pdbs = fields[2].upper().split('; ')
                    elif category == 'DRUGINFO':
                        drug_id = fields[2].upper()
                        drug_name = fields[3]
                        clinical_status = fields[3].lower()
                        self.drug_ids.add(drug_id)
                        self.targetid_to_drugids.setdefault(target_id, set()).add(drug_id)
                        self.drugid_to_drugname[drug_id] = drug_name
                        self.drugid_to_clinicalstatus[drug_id] = clinical_status
                    elif category == 'KEGGPATH':
                        kegg_path = fields[2].lower() # e.g. hsa04014:Ras signaling pathway
                        self.targetid_to_keggpath.setdefault(target_id, set()).add(kegg_path)
                    elif category == 'WIKIPATH':
                        wiki_path = fields[2] # e.g. WP1911:Signaling by FGFR
                    elif category == 'REACPATH':
                        reactome_path = fields[2] # e.g. R-HSA-1250196:SHC1 events in ERBB2 signaling => In BIANA, the reactome ID is only the number, but maybe it's not necessary to parse it

        #print(self.targetid_to_keggpath)

        return


    def parse_crossmatching(self):
        """
        Parsing TTD crossmatching of drugs
        """

        print("\n.....PARSING DRUG CROSSMATCHING FILE.....\n")

        with open(self.p1_03_crossmatching_file, 'r') as p1_03_crossmatching_fd:

            csvreader = csv.reader(p1_03_crossmatching_fd, delimiter='\t')

            for fields in csvreader:

                if len(fields) >= 3:
                    # FIELD1 = TARGETID / FIELD2 = CATEGORY / FIELD3 = VALUE
                    # D00AEQ	TTDDRUID	D00AEQ
                    drug_id = fields[0].upper()
                    category = fields[1]
                    self.drug_ids.add(drug_id)
                    if category == 'DRUGNAME':
                        drug_name = fields[2]
                        self.drugid_to_drugname[drug_id] = drug_name
                    elif category == 'PUBCHCID':
                        pubchem_cids = fields[2].split('; ')
                        for pubchem_cid in pubchem_cids:
                            if pubchem_cid != '':
                                self.drugid_to_pubchem_cids.setdefault(drug_id, set()).add(pubchem_cid)
                    elif category == 'PUBCHSID':
                        pubchem_sids = fields[2].split('; ')
                        for pubchem_sid in pubchem_sids:
                            if pubchem_sid != '':
                                self.drugid_to_pubchem_sids.setdefault(drug_id, set()).add(pubchem_sid)
                    elif category == 'CHEBI_ID':
                        chebi_ids = fields[2].upper().split('; ')
                        for chebi_id in chebi_ids:
                            if chebi_id != '':
                                chebi_id = chebi_id.lstrip() # Remove spaces at the beginning of the string
                                if chebi_id.startswith('CHEBI:'):
                                    chebi_id = chebi_id.lstrip().split('CHEBI:')[1]
                                    self.drugid_to_chebis.setdefault(drug_id, set()).add(chebi_id)
                                elif chebi_id.startswith('CHEMBL'):
                                    chebi_id = chebi_id.lstrip()
                                    self.drugid_to_chembls.setdefault(drug_id, set()).add(chebi_id)
                                elif chebi_id == '':
                                    continue
                                else:
                                    print('Unrecognized CHEBI ID for drug {}: {}'.format(drug_id, chebi_id))
                                    sys.exit(10)
                    elif category == 'SUPDRATC':
                        atcs = fields[2].upper().split('; ')
                        for atc in atcs:
                            if atc != '':
                                self.drugid_to_atcs.setdefault(drug_id, set()).add(atc)

        return


    def parse_drug_synonyms(self):
        """
        Parsing TTD synonyms of drugs
        """

        print("\n.....PARSING DRUG SYNONYMS FILE.....\n")

        with open(self.p1_04_synonyms_file, 'r') as p1_04_synonyms_fd:

            csvreader = csv.reader(p1_04_synonyms_fd, delimiter='\t')

            for fields in csvreader:

                if len(fields) >= 3:
                    # FIELD1 = TARGETID / FIELD2 = CATEGORY / FIELD3 = VALUE
                    drug_id = fields[0].upper()
                    category = fields[1]
                    self.drug_ids.add(drug_id)
                    if category == 'DRUGNAME':
                        drug_name = fields[2]
                        self.drugid_to_drugname[drug_id] = drug_name
                    elif category == 'SYNONYMS':
                        synonym = fields[2]
                        if synonym.startswith('CHEBI:'):
                            chebi_id = synonym.lstrip().split('CHEBI:')[1]
                            self.drugid_to_chebis.setdefault(drug_id, set()).add(chebi_id)
                        elif synonym.startswith('CHEMBL'):
                            chembl_id = synonym.lstrip()
                            self.drugid_to_chembls.setdefault(drug_id, set()).add(chembl_id)
                        elif synonym == '':
                            continue
                        else:
                            self.drugid_to_drugsynonyms.setdefault(drug_id, set()).add(synonym)

        #print(self.drugid_to_drugsynonyms)

        return


    def parse_drug_disease(self):
        """
        Parsing TTD drug disease info
        """

        print("\n.....PARSING DRUG TO DISEASE FILE.....\n")

        with open(self.p1_05_drug_disease_file, 'r') as p1_05_drug_disease_fd:

            csvreader = csv.reader(p1_05_drug_disease_fd, delimiter='\t')

            for fields in csvreader:

                if len(fields) >= 3:
                    # FIELD1 = TARGETID / FIELD2 = CATEGORY / FIELD3 = VALUE
                    # INDICATI	Indication	Disease entry	ICD-11	Clinical status
                    # D01FFA	DRUGNAME	(+-)-tetrahydropalmatine
                    # D01FFA	INDICATI	Analgesia [ICD-11: MB40.8] Approved
                    drug_id = fields[0].upper()
                    category = fields[1]
                    self.drug_ids.add(drug_id)
                    if category == 'DRUGNAME':
                        drug_name = fields[2]
                        self.drugid_to_drugname[drug_id] = drug_name
                    elif category == 'INDICATI': # Multiple indications in separate lines
                        indication_content = fields[2]
                        x = re.search("(.+)\s\[ICD-11: (.+)\]\s(.+)", indication_content)
                        if x:
                            indication = x.group(1)
                            icd_11 = x.group(2).upper()
                            clinical_status = x.group(3).lower()
                            self.drugid_to_icd_to_status.setdefault(drug_id, {})
                            self.drugid_to_icd_to_status[drug_id][icd_11] = clinical_status
                            self.icd_to_indication[icd_11] = indication
                        else:
                            print('The indication "{}" of drug {} does not match!'.format(indication_content, drug_id))
                            sys.exit(10)

        #print(self.drugid_to_icd_to_status)

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

