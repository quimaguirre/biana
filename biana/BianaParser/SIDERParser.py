from bianaParser import *
import gzip
                    
class SIDERParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from SIDER

    """                 
                                                                                         
    name = "sider"
    description = "This file implements a program that fills up tables in BIANA database from data in SIDER"
    external_entity_definition = "An external entity represents a phenotype, a side effect or a"
    external_entity_relations = "An external relation represents a drug combination"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "SIDER parser",  
                             default_script_name = "SIDERParser.py",
                             default_script_description = SIDERParser.description,     
                             additional_compulsory_arguments = [])
        self.default_eE_attribute = "pubchemcompound"
                    
    def parse_database(self):
        """                                                                              
        Method that implements the specific operations of a MyData formatted file
        """

        #####################################
        #### DEFINE NEW BIANA CATEGORIES ####
        #####################################

        # Add a different type of external entity
        self.biana_access.add_valid_external_entity_type( type = "drug" )
        self.biana_access.add_valid_external_entity_type( type = "phenotype" )
        self.biana_access.add_valid_external_entity_type( type = "disease" )

        # Add a different type of relation
        self.biana_access.add_valid_external_entity_relation_type( type = "drug_phenotype_association" )
        self.biana_access.add_valid_external_entity_relation_type( type = "drug_indication_association" )

        # Add different type of external entity attributes

        self.biana_access.add_valid_external_entity_attribute_type( name = "UMLS_CUI",
                                                                        data_type = "varchar(30)",
                                                                        category = "eE identifier attribute")

        # Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
        self.biana_access.refresh_database_information()



        ###########################
        #### CHECK INPUT FILES ####
        ###########################

        # Check that the input path exists
        if os.path.isdir(self.input_file):
            self.input_path = self.input_file
        else:
            raise ValueError("You must specify a path instead of a file")

        # Check that the necessary files exist
        self.meddra_file = os.path.join(self.input_path, 'meddra.tsv.gz')
        self.side_effect_file = os.path.join(self.input_path, 'meddra_all_label_se.tsv.gz')
        self.indication_file = os.path.join(self.input_path, 'meddra_all_label_indications.tsv.gz')

        if not os.path.isfile(self.meddra_file):
            print('The file \'meddra.tsv.gz\' is not in the input path!')
            sys.exit(10)

        if not os.path.isfile(self.side_effect_file):
            print('The file \'meddra_all_label_se.tsv.gz\' is not in the input path!')
            sys.exit(10)

        if not os.path.isfile(self.indication_file):
            print('The file \'meddra_all_label_indications.tsv.gz\' is not in the input path!')
            sys.exit(10)



        ########################
        #### PARSE DATABASE ####
        ########################

        # Parse the database
        parser = SIDER(self.input_path)
        print("\n.....PARSING MEDDRA.....\n")
        parser.parse_meddra()
        print("\n.....PARSING SIDE EFFECTS.....\n")
        parser.parse_side_effects()
        print("\n.....PARSING INDICATIONS.....\n")
        parser.parse_indications()



        #########################################
        #### INSERT PARSED DATABASE IN BIANA ####
        #########################################

        # Defining the dict in which we will store the created external entities
        self.external_entity_ids_dict = {}


        print("\n.....INSERTING PHENOTYPES (SIDE-EFFECTS).....\n")

        for umls in parser.phenotype_umls2pref:

            # Create an external entity corresponding to the disease in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(umls):

                #print("Adding phenotype %s" %(umls))
                self.create_phenotype_external_entity(parser, umls)


        print("\n.....INSERTING DRUGS.....\n")

        for pubchem in parser.pubchem_to_umls_cids:
            if not self.external_entity_ids_dict.has_key(pubchem):
                #print("Adding drug %s" %(pubchem))
                self.create_drug_external_entity(parser, pubchem)
        for pubchem in parser.indication_pubchem_to_umls_cids:
            if not self.external_entity_ids_dict.has_key(pubchem):
                #print("Adding drug %s" %(pubchem))
                self.create_drug_external_entity(parser, pubchem)


        print("\n.....INSERTING THE DRUG-PHENOTYPE ASSOCIATIONS.....\n")

        for pubchem in parser.pubchem_to_umls_cids:
            for umls in parser.pubchem_to_umls_cids[pubchem]:
                drug_phenotype = '{}___{}'.format(pubchem, umls)

                if not self.external_entity_ids_dict.has_key(drug_phenotype):
                    if pubchem in self.external_entity_ids_dict and umls in self.external_entity_ids_dict:
                        #print("Adding drug-phenotype association: {}".format(drug_phenotype))
                        self.create_drug_phenotype_external_entity(parser, drug_phenotype, 'phenotype')
                    else:
                        if pubchem not in self.external_entity_ids_dict:
                            print('Drug not found: {}'.format(pubchem))
                        if umls not in self.external_entity_ids_dict:
                            print('Phenotype not found: {}'.format(umls))


        print("\n.....INSERTING THE DRUG-INDICATION ASSOCIATIONS.....\n")

        drug_indication_associations = set()
        for pubchem in parser.indication_pubchem_to_umls_cids:
            for umls in parser.indication_pubchem_to_umls_cids[pubchem]:
                drug_indication = '{}___{}'.format(pubchem, umls)
                if not drug_indication in drug_indication_associations:
                    drug_indication_associations.add(drug_indication)
                    if pubchem in self.external_entity_ids_dict and umls in self.external_entity_ids_dict:
                        #print("Adding drug-indication association: {}".format(drug_indication))
                        self.create_drug_phenotype_external_entity(parser, drug_indication, 'indication')
                    else:
                        if pubchem not in self.external_entity_ids_dict:
                            print('Drug not found: {}'.format(pubchem))
                        if umls not in self.external_entity_ids_dict:
                            print('Indication not found: {}'.format(umls))


        print("\nPARSING OF SIDER FINISHED. THANK YOU FOR YOUR PATIENCE\n")

        return



    def create_phenotype_external_entity(self, parser, umls):
        """
        Create an external entity of a phenotype and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "phenotype" )

        # Annotate its UMLS
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UMLS_CUI", value=umls, type="unique" ) )

        # Associate its name
        if umls in parser.phenotype_umls2pref:
            preferred_term = parser.phenotype_umls2pref[umls].lower()
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Name", value=preferred_term, type="unique") )

            if preferred_term in parser.phenotype_pref2synonyms:
                for synonym in parser.phenotype_pref2synonyms[preferred_term]:
                    synonym = synonym.lower()
                    if synonym != preferred_term:
                        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Name", value=synonym, type="synonym") )
        else:
            print("Name not available for %s" %(umls))
            pass

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[umls] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_drug_external_entity(self, parser, pubchem):
        """
        Create an external entity of a gene and add it in BIANA
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "drug" )

        # Annotate its pubchem
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "PubChemCompound", value=pubchem, type="unique") )

        # Insert this external entity into BIANA
        self.external_entity_ids_dict[pubchem] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        return


    def create_drug_phenotype_external_entity(self, parser, drug_phenotype, phenotype_or_indication):
        """
        Create an external entity relation of a gene-disease association and add it in BIANA
        """

        pubchem, umls = drug_phenotype.split('___')

        # CREATE THE EXTERNAL ENTITY RELATION
        # Create an external entity relation corresponding to association between drug and phenotype/indication in database
        if phenotype_or_indication == 'phenotype':
            new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "drug_phenotype_association" )
        elif phenotype_or_indication == 'indication':
            new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "drug_indication_association" )

        # Add the drug in the association
        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[pubchem] )

        # Add the phenotype in the association
        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[umls] )

        # Insert this external entity relation into database
        self.external_entity_ids_dict[drug_phenotype] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return




class SIDER(object):
    """
    Parser class of the Side Effects database (SIDER)
    """

    def __init__(self, sider_dir):

        # MedDRA files
        self.meddra_file = os.path.join(sider_dir, 'meddra.tsv.gz')
        self.phenotype_preferred_names = set()
        self.phenotype_synonym2pref = {}
        self.phenotype_synonym2umls = {}
        self.phenotype_pref2umls = {}
        self.phenotype_pref2synonyms = {}
        self.phenotype_umls2pref = {}
        self.phenotype_umls2synonyms = {}

        # Side effect files
        self.side_effect_file = os.path.join(sider_dir, 'meddra_all_label_se.tsv.gz')
        self.pubchem_to_umls_cids = {}
        self.pubchem_to_side_effects = {}

        # Indication files
        self.indication_file = os.path.join(sider_dir, 'meddra_all_label_indications.tsv.gz')
        self.indication_pubchem_to_umls_cids = {}
        self.pubchem_to_indications = {}

        return



    def parse_meddra(self):
        """
        Parse the "meddra.tsv.gz" file, obtaining the UMLS IDs, MedDRA IDs, type of term and name
        """
        meddra_file_fd = gzip.open(self.meddra_file)
        umls_to_meddra = {}
        meddra_to_termname = {}
        termname_to_termtypes = {}
        termname_to_meddra = {}
        meddra_to_umls = {}
        term_types = set()
        for line in meddra_file_fd.readlines():
            umls, term_type, meddra, term_name = line.strip("\n").split("\t")
            umls = umls.upper()
            term_type = term_type.upper()
            term_name = term_name.lower()

            umls_to_meddra.setdefault(umls, set())
            umls_to_meddra[umls].add(meddra)
            meddra_to_termname[meddra] = term_name
            termname_to_termtypes.setdefault(term_name, set())
            termname_to_termtypes[term_name].add(term_type)
            termname_to_meddra[term_name] = meddra
            meddra_to_umls[meddra] = umls
            self.phenotype_synonym2umls[term_name] = umls
            self.phenotype_umls2synonyms.setdefault(umls, set())
            self.phenotype_umls2synonyms[umls].add(term_name)

            if term_type == 'PT':
                self.phenotype_preferred_names.add(term_name)
                self.phenotype_pref2umls[term_name] = umls
                self.phenotype_umls2pref[umls] = term_name

        for term_name in termname_to_meddra:

            term_types = termname_to_termtypes[term_name]
            if 'PT' in term_types:
                self.phenotype_synonym2pref[term_name] = term_name
                self.phenotype_pref2synonyms.setdefault(term_name, set())
                self.phenotype_pref2synonyms[term_name].add(term_name)
                continue
            umls_id = meddra_to_umls[termname_to_meddra[term_name]]
            if umls_id in umls_to_meddra:
                for curr_meddra_id in umls_to_meddra[umls_id]:
                    if curr_meddra_id in meddra_to_termname and meddra_to_termname[curr_meddra_id] in termname_to_termtypes and 'PT' in termname_to_termtypes[meddra_to_termname[curr_meddra_id]]:
                        self.phenotype_synonym2pref[term_name] = meddra_to_termname[curr_meddra_id]
                        self.phenotype_pref2synonyms.setdefault(meddra_to_termname[curr_meddra_id], set())
                        self.phenotype_pref2synonyms[meddra_to_termname[curr_meddra_id]].add(term_name)
                        break

        meddra_file_fd.close()
        return


    def parse_side_effects(self):
        """
        Parse the "meddra_all_label_se.tsv.gz" file, obtaining the relationships
        between compounds and side effects
        """
        side_effect_file_fd = gzip.open(self.side_effect_file)
        #cid_to_descriptions = {}
        for line in side_effect_file_fd.readlines():
            words = line.strip("\n").split("\t")
            # Get Pubchem id (Strip "CID")
            cid_flat = "%s" % (abs(int(words[1][3:])) - 100000000)
            cid_specific = "%s" % abs(int(words[2][3:])) 
            try:
                term_type, term_cid, term_name = words[4:]
            except:
                print line
                print words
                term_type, term_cid, term_name = words[4:]
            if term_type != "PT":
                continue
            self.pubchem_to_umls_cids.setdefault(cid_flat, set()).add(term_cid) 
            self.pubchem_to_umls_cids.setdefault(cid_specific, set()).add(term_cid) 
            #cid_to_descriptions.setdefault(term_cid, set()).add(term_name.lower())
            self.pubchem_to_side_effects.setdefault(cid_flat, set()).add(term_name)
            self.pubchem_to_side_effects.setdefault(cid_specific, set()).add(term_name) 
        side_effect_file_fd.close()
        return


    def parse_indications(self):
        """
        Parse the "meddra_all_label_indications.tsv.gz" file, obtaining the 
        relationships between compounds and indications
        """
        indication_file_fd = gzip.open(self.indication_file)
        for line in indication_file_fd.readlines():
            words = line.strip("\n").split("\t")
            # Get Pubchem id (Strip "CID")
            cid_flat = "%s" % (abs(int(words[1][3:])) - 100000000)
            cid_specific = "%s" % abs(int(words[2][3:])) 
            term_type, term_cid, term_name = words[6:]
            if term_type != "PT":
                continue
            self.indication_pubchem_to_umls_cids.setdefault(cid_flat, set()).add(term_cid) 
            self.indication_pubchem_to_umls_cids.setdefault(cid_specific, set()).add(term_cid) 
            self.pubchem_to_indications.setdefault(cid_flat, set()).add(term_name)
            self.pubchem_to_indications.setdefault(cid_specific, set()).add(term_name) 
        indication_file_fd.close()
        return



if __name__ == "__main__":
    main()