from bianaParser import *
                    
class dcdbParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from DrugBank

    """                 
                                                                                         
    name = "dcdb"
    description = "This file implements a program that fills up tables in BIANA database from data in DCDB"
    external_entity_definition = "An external entity represents a drug or a protein"
    external_entity_relations = "An external relation represents a drug combination or an interaction between drugs or drug-target"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "DCDB parser",  
                             default_script_name = "dcdbParser.py",
                             default_script_description = dcdbParser.description,     
                             additional_compulsory_arguments = [])
                    
    def parse_database(self):
        """                                                                              
        Method that implements the specific operations of a MyData formatted file
        """

        # # Add DCDB_combinationID as a valid external entity attribute since it is not recognized by BIANA
        # self.biana_access.add_valid_external_entity_attribute_type( name = "DCDB_combinationID",
        #                                                             data_type = "varchar(370)",
        #                                                             category = "eE identifier attribute")

        # # Add DCDB_drugID as a valid external entity attribute since it is not recognized by BIANA
        # self.biana_access.add_valid_external_entity_attribute_type( name = "DCDB_drugID",
        #                                                             data_type = "varchar(370)",
        #                                                             category = "eE identifier attribute")

        # # Add DCDB_targetID as a valid external entity attribute since it is not recognized by BIANA
        # self.biana_access.add_valid_external_entity_attribute_type( name = "DCDB_targetID",
        #                                                             data_type = "varchar(370)",
        #                                                             category = "eE identifier attribute")

        # # Add ATC as a valid external entity attribute since it is not recognized by BIANA
        # self.biana_access.add_valid_external_entity_attribute_type( name = "ATC",
        #                                                             data_type = "varchar(370)",
        #                                                             category = "eE identifier attribute")

        # # Add DrugBankID as a valid external entity attribute since it is not recognized by BIANA
        # self.biana_access.add_valid_external_entity_attribute_type( name = "DrugBankID",
        #                                                             data_type = "varchar(370)",
        #                                                             category = "eE identifier attribute")

        data_type_dict = {"fields": [ ("value","varchar(50)"),
                                     ("interactionType","ENUM(\"pharmacodynamical\",\"pharmacokinetical\")",False),
                                     ("classification","varchar(255)", False)
                                   ],
                          "indices": ("value",)} # Stores a regex


        # Add different type of external entity attribute
        self.biana_access.add_valid_external_entity_attribute_type( name = "DCDB_druginteractionID",
                                                                        data_type = data_type_dict,
                                                                        category = "eE special attribute")


        # # Add a different type of relation type. In this case, it will be "drug combination", as it is different from a drug-drug interaction 
        # self.biana_access.add_valid_external_entity_relation_type( type = "drug_combination" )

        # # Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
        # self.biana_access.refresh_database_information()


        # Check that all the files exist
        if os.path.isdir(self.input_file):
            self.input_path = self.input_file
        else:
            raise ValueError("You must specify a path instead of a file")

        files = ["COMPONENTS.txt", "DC_TO_COMPONENTS.txt", "DRUG_COMBINATION.txt", 
                 "DCC_TO_TARGETS.txt", "TARGETS.txt",
                 "DCC_TO_ATC.txt", "ATC_CODES.txt",
                 "DCC_TO_OUTLINK.txt", "DRUG_OUTLINK.txt",
                 "DC_TO_INTERACTION.txt", "DRUG_INTERACTION.txt"]

        for current_file in files:
            if os.path.exists(self.input_path+os.sep+"dcdb"+os.sep+current_file) is False:
                raise ValueError("File %s is missing in %s" %(current_file, self.input_path))

        parser = DCDB(self.input_path)
        parser.parse()


        print("\n.....INSERTING THE COMPONENT DRUGS IN THE DATABASE.....\n")

        self.external_entity_ids_dict = {}

        for component in parser.components:

            # Create an external entity corresponding to the drug in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(component):

                #print("Adding component drug %s" %(component))
                self.create_component_external_entity(parser, component)


        print("\n.....INSERTING THE DRUG COMBINATIONS IN THE DATABASE.....\n")

        for combination in parser.combinations:

            #print("Adding combination: {}".format(combination))

            # CREATE THE EXTERNAL ENTITY RELATION
            # Create an external entity relation corresponding to interaction between Uniprot_id1 and Uniprot_id2 in database
            new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "drug_combination" )

            for component in parser.combination2component[combination]:

                #print("Adding component: {}".format(component))

                # Associate the components as participants of the interaction
                new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[component] )

            # Associate the drug combination id, name and mechanism of the combination with this relation
            new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "DCDB_combinationID",
                                                                                                                 value = combination, type = "unique" ) )
            if combination in parser.combination2name:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "Name",
                                                                                                                     value = parser.combination2name[combination], type="cross-reference" ) )
            if combination in parser.combination2mechanism:
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "Description",
                                                                                                                     value = parser.combination2mechanism[combination] ) )

            # Insert this external entity relation into database
            self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )


        print("\n.....INSERTING THE DRUG INTERACTIONS IN THE DATABASE.....\n")

        for interaction in parser.interactions:

            #print("Adding interaction: {}".format(interaction))

            # CREATE THE EXTERNAL ENTITY RELATION
            # Create an external entity relation corresponding to interaction between two drugs in the database
            new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "interaction" )

            for component in parser.interaction2component[interaction]:

                #print("Adding component: {}".format(component))

                # Associate the components as participants of the interaction
                new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[component] )

            interaction_type = parser.interaction2type[interaction].lower()
            classification = parser.interaction2classification[interaction]

            # Associate the drug interaction id, type and classification with this relation
            new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier= "DCDB_druginteractionID", value=interaction.upper(), type="unique",
                                                                                         additional_fields = {"interactionType": interaction_type, "classification": classification} ) )

            # Insert this external entity relation into database
            self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

        return


    def create_component_external_entity(self, parser, component):
        """
        Create a complete external entity of a component (drug component of a drug combination)
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "drug" )

        # Annotate it as DCDB_drugID
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DCDB_drugID", value=component, type="unique") )

        # Associate its name
        if component in parser.component2name:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Name", value=parser.component2name[component], type="cross-reference") )
        else:
            print("Name not available for %s" %(component))
            pass

        # Associate its ATC codes
        if component in parser.component2atcid:
            for atcid in parser.component2atcid[component]:
                atccode = parser.atcid2atccode[atcid]
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "ATC", value=atccode.upper(), type="cross-reference" ) )
        else:
            #print("ATC codes not available for %s" %(component))
            pass

        # Associate its OUTLINKS

        drugbank_regex = re.compile("^DB[0-9]{5}")

        if component in parser.component2outlink:
            for outlink in parser.component2outlink[component]:
                source = parser.outlink2sourcelink[outlink]['source']
                link = parser.outlink2sourcelink[outlink]['link']
                # Distinct sources --> 'bindingdb', 'pubchem substance', 'pharmgkb', 'wikipedia', 'drugbank', 'kegg compound', 'pubchem compound', 'rxlist', 'chebi', 'pdb', 'kegg drug'
                if source == 'drugbank':
                    m = drugbank_regex.match(link.upper())
                    if m:
                        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DrugBankID", value=link.upper(), type="cross-reference" ) )
                elif source == 'pubchem compound':
                    new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "PubChemCompound", value=link.upper(), type="cross-reference" ) )
                elif source == 'chebi':
                    new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "CHEBI", value=link.upper(), type="cross-reference" ) )
        else:
            #print("Outlinks not available for %s" %(component))
            pass


        # Insert this external entity into database
        self.external_entity_ids_dict[component] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        # Associate its targets
        if component in parser.component2target:

            for target in parser.component2target[component]:

                if not self.external_entity_ids_dict.has_key(target):
                    target_external_entity = ExternalEntity( source_database = self.database, type = "protein" )
                    target_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DCDB_targetID", value=target, type="unique") )

                    if target in parser.target2genename:
                        target_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Name", value=parser.target2genename[target], type="cross-reference") )
                        #print("Target Gene: %s" %(parser.target2genename[target]))
                    if target in parser.target2genesymbol:
                        target_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneSymbol", value=parser.target2genesymbol[target], type="cross-reference") )
                        #print("Target Gene: %s" %(parser.target2genesymbol[target]))
                    if target in parser.target2geneid:
                        target_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneID", value=parser.target2geneid[target], type="cross-reference") )
                        #print("Target Gene: %s" %(parser.target2geneid[target]))
                    if target in parser.target2uniprotacc:
                        target_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotAccession", value=parser.target2uniprotacc[target], type="cross-reference") )
                        #print("Target Uniprot: %s" %(parser.target2uniprotacc[target]))
                    if target in parser.target2taxid:
                        target_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "TaxID", value=parser.target2taxid[target], type="cross-reference") )
                        #print("Target Uniprot: %s" %(parser.target2taxid[target]))
                    if target in parser.target2hgnc:
                        target_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "HGNC", value=parser.target2hgnc[target], type="cross-reference") )
                        #print("Target Uniprot: %s" %(parser.target2hgnc[target]))

                    self.external_entity_ids_dict[target] = self.biana_access.insert_new_external_entity( externalEntity = target_external_entity )

                # Create an external entity relation corresponding to interaction between the drug and the protein in database
                new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "interaction" )
                # Associate drug as the first participant in this interaction
                new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[component] )
                # Associate target as the second participant in this interaction
                new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[target] )

                # Insert this external entity realtion into database
                self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )


        else:
            #print("Targets not available for %s" %(component))
            pass


        return



class DCDB(object):

    def __init__(self, path):

        self.components_file = path + "/dcdb/" + "COMPONENTS.txt"
        self.dc2components_file = path + "/dcdb/" + "DC_TO_COMPONENTS.txt"
        self.drug_combination_file = path + "/dcdb/" + "DRUG_COMBINATION.txt"
        self.dcc2targets_file = path + "/dcdb/" + "DCC_TO_TARGETS.txt"
        self.targets_file = path + "/dcdb/" + "TARGETS.txt"
        self.dcc2atc_file = path + "/dcdb/" + "DCC_TO_ATC.txt"
        self.atc_file = path + "/dcdb/" + "ATC_CODES.txt"
        self.dcc2outlink_file = path + "/dcdb/" + "DCC_TO_OUTLINK.txt"
        self.outlink_file = path + "/dcdb/" + "DRUG_OUTLINK.txt"
        self.dc2interaction_file = path + "/dcdb/" + "DC_TO_INTERACTION.txt"
        self.interaction_file = path + "/dcdb/" + "DRUG_INTERACTION.txt"

        self.components = set()
        self.component2name = {}
        self.component2target = {}
        self.component2atcid = {}
        self.component2outlink = {}

        self.combinations = set()
        self.combination2component = {}
        self.combination2name = {}
        self.combination2mechanism = {}

        self.target2genename = {}
        self.target2genesymbol = {}
        self.target2geneid = {}
        self.target2uniprotacc = {}
        self.target2taxid = {}
        self.target2hgnc = {}

        self.atcid2atccode = {}

        self.outlink2sourcelink = {}

        self.interactions = set()
        self.interaction2combination = {}
        self.interaction2type = {}
        self.interaction2classification = {}
        self.interaction2component = {}


        return

    def parse(self):

        ###############################
        #### PARSE COMPONENTS FILE ####
        ###############################

        print("\n.....PARSING COMPONENTS FILE.....\n")

        components_file_fd = open(self.components_file,'r')

        first_line = components_file_fd.readline()

        # Obtain a dictionary: "field_name" => "position"
        fields_dict = self.obtain_header_fields(first_line)

        # Obtain the component ID and the component name
        # Introduce them in the dictionary component2name: "component_id" => "component_name"
        for line in components_file_fd:
            fields = line.strip().split("\t")
            component = fields[ fields_dict['dcc_id'] ]
            name = fields[ fields_dict['generic_name'] ]
            self.component2name[component] = name
            self.components.add(component)

        components_file_fd.close()
        #print(self.components)
        #print(self.component2name)

        ####################################
        #### PARSE DC 2 COMPONENTS FILE ####
        ####################################

        print("\n.....PARSING DC_TO_COMPONENTS FILE.....\n")

        dc2components_file_fd = open(self.dc2components_file,'r')

        first_line = dc2components_file_fd.readline()

        # Obtain a dictionary: "field_name" => "position"
        fields_dict = self.obtain_header_fields(first_line)

        # Obtain the component ID and the combination id
        # Introduce them in the dictionary combination2component: "combination_id" => "[component_id1, ... , component_idn]"
        for line in dc2components_file_fd:
            fields = line.strip().split("\t")
            combination = fields[ fields_dict['dc_id'] ]
            component = fields[ fields_dict['dcc_id'] ]
            self.combination2component.setdefault(combination, [])
            self.combination2component[combination].append(component)

        dc2components_file_fd.close()
        #print(self.combination2component)


        ################################
        #### PARSE COMBINATION FILE ####
        ################################

        print("\n.....PARSING COMBINATION FILE.....\n")

        drug_combination_file_fd = open(self.drug_combination_file,'r')

        first_line = drug_combination_file_fd.readline()

        fields_dict = self.obtain_header_fields(first_line)

        for line in drug_combination_file_fd:
            fields = line.strip().split("\t")
            combination = fields[ fields_dict['dc_id'] ]
            comb_name = fields[ fields_dict['brand_name'] ]
            mechanism = fields[ fields_dict['mechanism'] ]
            if mechanism != "null" and mechanism != "":
                self.combination2mechanism[combination] = mechanism
            if comb_name != "null" and comb_name != "":
                self.combination2name[combination] = comb_name
            self.combinations.add(combination)

        drug_combination_file_fd.close()
        #print(self.combinations)
        #print(self.combination2mechanism)
        #print(self.combination2name)


        ##################################
        #### PARSE DCC 2 TARGETS FILE ####
        ##################################

        print("\n.....PARSING DCC_TO_TARGETS FILE.....\n")

        dcc2targets_file_fd = open(self.dcc2targets_file,'r')

        first_line = dcc2targets_file_fd.readline()

        # Obtain a dictionary: "field_name" => "position"
        fields_dict = self.obtain_header_fields(first_line)

        # Obtain the component ID and the combination id
        # Introduce them in the dictionary combination2component: "combination_id" => "[component_id1, ... , component_idn]"
        for line in dcc2targets_file_fd:
            fields = line.strip().split("\t")
            component = fields[ fields_dict['dcc_id'] ]
            target = fields[ fields_dict['tar_id'] ]
            self.component2target.setdefault(component, [])
            self.component2target[component].append(target)

        dcc2targets_file_fd.close()
        #print(self.component2target)


        ############################
        #### PARSE TARGETS FILE ####
        ############################

        print("\n.....PARSING TARGETS FILE.....\n")

        targets_file_fd = open(self.targets_file,'r')

        first_line = targets_file_fd.readline()

        fields_dict = self.obtain_header_fields(first_line)

        for line in targets_file_fd:
            fields = line.strip().split("\t")
            target = fields[ fields_dict['tar_id'] ]
            genename = fields[ fields_dict['genename'] ]
            gene_symbol = fields[ fields_dict['gene_symbol'] ]
            geneid = fields[ fields_dict['gene_id'] ]
            uniprotacc = fields[ fields_dict['uniprot_accessionnumber'] ]
            taxid = fields[ fields_dict['taxon_number'] ]
            hgnc = fields[ fields_dict['hgnc_id'] ][5:]
            if genename != "null" and genename != "":
                self.target2genename[target] = genename
            if gene_symbol != "null" and gene_symbol != "":
                self.target2genesymbol[target] = gene_symbol
            if geneid != "null" and geneid != "":
                self.target2geneid[target] = geneid
            if uniprotacc != "null" and uniprotacc != "":
                self.target2uniprotacc[target] = uniprotacc
            if taxid != "null" and taxid != "":
                self.target2taxid[target] = taxid
            if hgnc != "null" and hgnc != "":
                self.target2hgnc[target] = hgnc

        targets_file_fd.close()
        #print(self.target2genename)
        #print(self.target2genesymbol)
        #print(self.target2geneid)
        #print(self.target2uniprotacc)
        #print(self.target2taxid)
        #print(self.target2hgnc)


        ##############################
        #### PARSE DCC 2 ATC FILE ####
        ##############################

        print("\n.....PARSING DCC_TO_ATC FILE.....\n")

        dcc2atc_file_fd = open(self.dcc2atc_file,'r')

        first_line = dcc2atc_file_fd.readline()

        # Obtain a dictionary: "field_name" => "position"
        fields_dict = self.obtain_header_fields(first_line)

        # Obtain the component ID and the combination id
        # Introduce them in the dictionary combination2component: "combination_id" => "[component_id1, ... , component_idn]"
        for line in dcc2atc_file_fd:
            fields = line.strip().split("\t")
            component = fields[ fields_dict['dcc_id'] ]
            atcid = fields[ fields_dict['dat_id'] ]
            self.component2atcid.setdefault(component, [])
            self.component2atcid[component].append(atcid)

        dcc2atc_file_fd.close()
        #print(self.component2atcid)


        ##############################
        #### PARSE ATC_CODES FILE ####
        ##############################

        print("\n.....PARSING ATC CODES FILE.....\n")

        atc_file_fd = open(self.atc_file,'r')

        first_line = atc_file_fd.readline()

        fields_dict = self.obtain_header_fields(first_line)

        for line in atc_file_fd:
            fields = line.strip().split("\t")
            atcid = fields[ fields_dict['dat_id'] ]
            atccode = fields[ fields_dict['code'] ]
            self.atcid2atccode[atcid] = atccode

        atc_file_fd.close()
        #print(self.atcid2atccode)


        ##################################
        #### PARSE DCC 2 OUTLINK FILE ####
        ##################################

        print("\n.....PARSING DCC_TO_OUTLINK FILE.....\n")

        dcc2outlink_file_fd = open(self.dcc2outlink_file,'r')

        first_line = dcc2outlink_file_fd.readline()

        # Obtain a dictionary: "field_name" => "position"
        fields_dict = self.obtain_header_fields(first_line)

        # Obtain the component ID and the combination id
        # Introduce them in the dictionary combination2component: "combination_id" => "[component_id1, ... , component_idn]"
        for line in dcc2outlink_file_fd:
            fields = line.strip().split("\t")
            component = fields[ fields_dict['dcc_id'] ]
            outlink = fields[ fields_dict['dol_id'] ]
            self.component2outlink.setdefault(component, [])
            self.component2outlink[component].append(outlink)

        dcc2outlink_file_fd.close()
        #print(self.component2outlink)


        #################################
        #### PARSE DRUG_OUTLINK FILE ####
        #################################

        print("\n.....PARSING DRUG_OUTLINK FILE.....\n")

        outlink_file_fd = open(self.outlink_file,'r')

        first_line = outlink_file_fd.readline()

        fields_dict = self.obtain_header_fields(first_line)

        # Distinct sources --> 'bindingdb', 'pubchem substance', 'pharmgkb', 'wikipedia', 'drugbank', 'kegg compound', 'pubchem compound', 'rxlist', 'chebi', 'pdb', 'kegg drug'

        for line in outlink_file_fd:
            fields = line.strip().split("\t")
            outlink = fields[ fields_dict['dol_id'] ]
            source = fields[ fields_dict['source'] ]
            link = fields[ fields_dict['link'] ]
            self.outlink2sourcelink.setdefault(outlink, {})
            self.outlink2sourcelink[outlink]['source'] = source.lower()
            self.outlink2sourcelink[outlink]['link'] = link

        outlink_file_fd.close()
        #print(self.outlink2sourcelink)


        #####################################
        #### PARSE DC 2 INTERACTION FILE ####
        #####################################

        print("\n.....PARSING DC_TO_INTERACTION FILE.....\n")

        dc2interaction_file_fd = open(self.dc2interaction_file,'r')

        first_line = dc2interaction_file_fd.readline()

        # Obtain a dictionary: "field_name" => "position"
        fields_dict = self.obtain_header_fields(first_line)

        # Obtain the interaction ID and the combination id
        # Introduce them in the dictionary combination2interaction: "combination_id" => "[interaction_id1, ... , interaction_idn]"
        for line in dc2interaction_file_fd:
            fields = line.strip().split("\t")
            combination = fields[ fields_dict['dc_id'] ]
            interaction = fields[ fields_dict['di_id'] ]
            self.interaction2combination.setdefault(interaction, [])
            self.interaction2combination[interaction].append(combination)

        dc2interaction_file_fd.close()
        #print(self.combination2interaction)


        #####################################
        #### PARSE DRUG INTERACTION FILE ####
        #####################################

        print("\n.....PARSING DRUG INTERACTION FILE.....\n")

        drug_interaction_file_fd = open(self.interaction_file,'r')

        first_line = drug_interaction_file_fd.readline()

        fields_dict = self.obtain_header_fields(first_line)

        for line in drug_interaction_file_fd:
            fields = line.strip().split("\t")
            interaction = fields[ fields_dict['di_id'] ]
            type_int = fields[ fields_dict['type'] ]
            #set(['Pharmacodynical', 'Pharmacokinetical', 'Pharmacodynamical'])
            if type_int == 'Pharmacodynical':
                type_int = 'Pharmacodynamical'
            classification = fields[ fields_dict['classification'] ]
            #set(['Enhancement of metabolism', 'Same target', 'Inhibition of metabolism', 'Different targets in same biological process', 'anti-acid in blood', 'Different targets of different biological processes', 'Different targets in related biological processes', 'Different targets in different biological processes', 'Inhibtion of metabolism'])
            if classification == 'Inhibtion of metabolism':
                classification = 'Inhibition of metabolism'
            if classification == 'Different targets of different biological processes':
                classification = 'Different targets in different biological processes'

            if type_int != "null" and type_int != "":
                self.interaction2type[interaction] = type_int
            if classification != "null" and classification != "":
                self.interaction2classification[interaction] = classification
            self.interactions.add(interaction)

            components = fields[ fields_dict['component'] ]
            (component1, component2) = components.split(' - ')

            compid1 = None
            compid2 = None

            component1 = self.check_exception(component1)
            component2 = self.check_exception(component2)

            for component in self.component2name:
                if self.component2name[component].lower() == component1.lower():
                    compid1 = component
                elif self.component2name[component].lower() == component2.lower():
                    compid2 = component
                elif component1.lower() in self.component2name[component].lower():
                    compid1 = component
                elif component2.lower() in self.component2name[component].lower():
                    compid2 = component

            if compid1 != None and compid2 != None:
                self.interaction2component.setdefault(interaction, set())
                self.interaction2component[interaction].add(compid1)
                self.interaction2component[interaction].add(compid2)
            else:
                if compid1 == None and compid2 == None:
                    print('The components {} and {} of the drug interaction {} do not have ID\n'.format(component1, component2, interaction))
                elif compid1 == None and compid2 != None:
                    print('The component {} of the drug interaction {} does not have ID\n'.format(component1, interaction))
                elif compid2 == None and compid1 != None:
                    print('The component {} of the drug interaction {} does not have ID\n'.format(component2, interaction))
                sys.exit(10)


        drug_interaction_file_fd.close()
        #print(self.interactions)
        #print(self.interaction2type)
        #print(self.interaction2classification)

        return


    def check_exception(self, drug_name):
        """ 
        Obtain a dictionary: "field_name" => "position" 
        """
        exception = {
            'Cholecacliferol':'Cholecalciferol',
            'Alemtuzumab':'Campath 1H',
            'Etanercept':'Enbrel',
            'LY294002 ':'LY294002',
            'Progesterone':'Progestrone',
            'Epoetin-alfa':'Epoetin-alpha',
            'Medroxyprogesterone acetate':'Medroxyprogeterone acetate',
            'Risperidone':'Risperidoene',
            'Mecasermin':'Iplex',
            'Insulin lispro':'Humalog',
            'Ec107 fabI':'Ec107fabI',
            '"Fluorouracil': 'Fluorouracil',
            'Idronoxil':'Phenoxodiol'
        }

        if drug_name in exception:
            drug_name = exception[drug_name]
        else:
            if len(drug_name.split(' ')) > 1:
                if drug_name.split(' ')[1] in ('hydrochloride','cypionate','trifenatate','sodium','acetate','kamedoxomil','hydrobromide'):
                    drug_name = drug_name.split(' ')[0]

        return drug_name


    def obtain_header_fields(self, first_line):
        """ 
        Obtain a dictionary: "field_name" => "position" 
        """
        fields_dict = {}

        header_fields = first_line.strip().split("\t")
        for x in xrange(0, len(header_fields)):
            fields_dict[header_fields[x].lower()] = x

        return fields_dict


if __name__ == "__main__":
    main()

