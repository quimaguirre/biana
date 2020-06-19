from bianaParser import *
from xml.etree.ElementTree import iterparse
import os, cPickle

class DrugBankParser(BianaParser):                                                        
    """             
    DrugBank Parser Class 

    Parses data from DrugBank

    """                 
                                                                                         
    name = "drugbank"
    description = "This file implements a program that fills up tables in BIANA database from data in DrugBank"
    external_entity_definition = "An external entity represents a drug or a protein"
    external_entity_relations = "An external relation represents an interaction between drugs or between drug and protein"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "DrugBank parser",  
                             default_script_name = "drugbankParser.py",
                             default_script_description = DrugBankParser.description,     
                             additional_compulsory_arguments = [])
        self.default_eE_attribute = "drugbankid"

    def parse_database(self):
        """                                                                              
        Method that implements the specific operations of a DrugBank formatted file
        """

		# group (e.g. approved, nutraceutical, investigational, withdrawn...)
        self.biana_access.add_valid_external_entity_attribute_type( name = "DrugBank_drug_group",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE descriptive attribute")

		# <drug type=""...> (e.g. small molecule, biotech...)
        self.biana_access.add_valid_external_entity_attribute_type( name = "DrugBank_drug_type",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE identifier attribute")

		# known-action (e.g. yes, no, unknown...)
        self.biana_access.add_valid_external_entity_attribute_type( name = "DrugBank_known_action",
                                                                    data_type = "text",
                                                                    category = "eE descriptive attribute")

		# action (e.g. inhibitor, antibody, substrate...)
        self.biana_access.add_valid_external_entity_attribute_type( name = "DrugBank_action",
                                                                    data_type = "text",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "DrugBank_mechanism_of_action",
                                                                    data_type = "text",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "DrugBank_indication",
                                                                    data_type = "text",
                                                                    category = "eE descriptive attribute")

		# Description of the toxicity of the drug
        self.biana_access.add_valid_external_entity_attribute_type( name = "DrugBank_toxicity",
                                                                    data_type = "text",
                                                                    category = "eE descriptive attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "DrugBank_DDI_description",
                                                                    data_type = "text",
                                                                    category = "eE descriptive attribute")

		# PubChem Substance
        self.biana_access.add_valid_external_entity_attribute_type( name = "PubChemSubstance",
                                                                    data_type = "int unsigned",
                                                                    category = "eE identifier attribute")


        # Mixtures
        data_type_dict = {"fields": [ ("value","varchar(370)"),
                                      ("ingredients","varchar(370)", False)
                                   ],
                          "indices": ("value",)} # Stores a regex

        # Add different type of external entity attribute
        self.biana_access.add_valid_external_entity_attribute_type( name = "DrugBank_mixtures",
                                                                        data_type = data_type_dict,
                                                                        category = "eE special attribute")

        # Adding two new data_type of external entity attribute
        self.biana_access.add_valid_identifier_reference_types('brand')
        self.biana_access.add_valid_identifier_reference_types('product')

        # Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
        self.biana_access.refresh_database_information()


        self.input_file_fd = open(self.input_file, 'r')
        self.external_entity_ids_dict = {}

        print(".....PARSING THE DATABASE. THIS CAN LAST SOME MINUTES.....")

        parser = DrugBankXMLParser(self.input_file)
        parser.parse()

        # ADD ALL THE INDIVIDUAL DRUG INFORMATION
        print(".....ADDING ALL THE INFORMATION OF THE INDIVIDUAL DRUGS.....")
        for drug in parser.drugs:

            # Create an external entity corresponding to the drug in the database (if it is not already created)
            if not self.external_entity_ids_dict.has_key(drug):

                #print("Adding main drug {}".format(drug))
                self.create_drug_external_entity(parser, drug)

        # ADD ALL THE DRUG-DRUG INTERACTIONS INFORMATION
        print(".....ADDING ALL THE INFORMATION OF THE DRUG-DRUG INTERACTIONS.....")
        added_ddis = set()
        for drug in parser.drugs:

                if drug in parser.drug_to_interactions:

                    ddis = parser.drug_to_interactions[drug] # Problem --> the dict only stores 1 interaction, not two!!!! Put them in a list

                    for ddi in ddis:

                        comb1 = '{0}---{1}'.format(drug, ddi)
                        comb2 = '{1}---{0}'.format(drug, ddi)

                        #print("Adding drug {}".format(ddi))
                        if self.external_entity_ids_dict.has_key(ddi):

                            if comb1 not in added_ddis and comb2 not in added_ddis:

                                        #print("Interaction with: {}".format(ddi))
                                        #print("Description: {}".format(ddis[ddi]))

                                        # CREATE THE EXTERNAL ENTITY RELATION
                                        # Create an external entity relation corresponding to interaction between Uniprot_id1 and Uniprot_id2 in database
                                        new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "interaction" )
                                        # Associate drug as the first participant in this interaction
                                        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[drug] )
                                        # Associate ddi as the second participant in this interaction
                                        new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[ddi] )
                                        # Associate the description of the interaction with this relation
                                        for description in ddis[ddi]:
                                            new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "DrugBank_DDI_description",
                                                                                                                                                 value = description, type="unique" ) )

                                        if ddi in parser.drug_to_interactions:
                                            inverse_ddi = parser.drug_to_interactions[ddi]
                                            if drug in inverse_ddi:
                                                for description in inverse_ddi[drug]:
                                                    if description not in ddis[ddi]:
                                                        new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "DrugBank_DDI_description",
                                                                                                                                                             value = description, type="unique" ) )

                                        # Insert this external entity realtion into database
                                        self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

                                        added_ddis.add(comb1)


        return


    def create_drug_external_entity(self, parser, drug):
        """
        Create a complete external entity of a drug
        """

        new_external_entity = ExternalEntity( source_database = self.database, type = "drug" )

        # Annotate it as DrugBankID
        new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DrugBankID", value=drug, type="unique") )

        # Associate its name
        if drug in parser.drug_to_name:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Name", value=parser.drug_to_name[drug], type="unique") )
        else:
            #print("Name not available for {}".format(drug))
            pass

        # Associate its synonyms
        if drug in parser.drug_to_synonyms:
            for synonym in parser.drug_to_synonyms[drug]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Name", value=synonym, type="synonym") )
        if drug in parser.drug_to_products:
            for product in parser.drug_to_products[drug]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Name", value=product, type="product") )
        if drug in parser.drug_to_brands:
            for brand in parser.drug_to_brands[drug]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Name", value=brand, type="brand") )

        # Associate its mixtures
        if drug in parser.drug_to_mixtures:
            for mixture in parser.drug_to_mixtures[drug]:
                if mixture in parser.mixture_to_ingredients:
                    ingredients = parser.mixture_to_ingredients[mixture]
                    new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DrugBank_mixtures", value=mixture, type="unique",
                                                                                additional_fields = {"ingredients": ingredients} ) )

        # Associate its description
        if drug in parser.drug_to_description:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Description", value=parser.drug_to_description[drug], type="unique") )
        else:
            #print("Description not available for {}".format(drug))
            pass

        # Associate its type
        if drug in parser.drug_to_type:
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DrugBank_drug_type", value=parser.drug_to_type[drug], type="unique") )
        else:
            #print("Drug type not available for {}".format(drug))
            pass

        # Associate its groups
        if drug in parser.drug_to_groups:
            for group in parser.drug_to_groups[drug]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DrugBank_drug_group", value=group, type="unique") )

        # Associate its indication
        if drug in parser.drug_to_indication:
            for indication in parser.drug_to_indication[drug]:
                if indication:
                    new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DrugBank_indication", value=indication, type="unique") )
        else:
            #print("INDICATION not available for {}".format(drug))
            pass

        # Associate its mechanism of action
        if drug in parser.drug_to_moa:
            if parser.drug_to_moa[drug]:
                #print("Mechanism of Action of drug {}: {}".format(drug, parser.drug_to_moa[drug]))
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DrugBank_mechanism_of_action", value=parser.drug_to_moa[drug], type="unique") )
        else:
            #print("Mechanism of action not available for {}".format(drug))
            pass

        # Associate its toxicity
        if drug in parser.drug_to_toxicity:
            if parser.drug_to_toxicity[drug]:
                #print("Toxicity of drug {}: {}".format(drug, parser.drug_to_toxicity[drug]))
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DrugBank_toxicity", value=parser.drug_to_toxicity[drug], type="unique") )
        else:
            #print("Toxicity not available for {}".format(drug))
            pass

        # Associate its PubChem Compound
        if drug in parser.drug_to_pubchem:
            #print("PubChem of drug {}: {}".format(drug, parser.drug_to_pubchem[drug]))
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "PubChemCompound", value=parser.drug_to_pubchem[drug], type="cross-reference") )
        else:
            #print("PubChem not available for {}".format(drug))
            pass

        # Associate its PubChem Substance
        if drug in parser.drug_to_pubchem_substance:
            #print("PubChem Substance of drug {}: {}".format(drug, parser.drug_to_pubchem_substance[drug]))
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "PubChemSubstance", value=parser.drug_to_pubchem_substance[drug], type="cross-reference") )
        else:
            #print("PubChem Substance not available for {}".format(drug))
            pass

        # Associate its ChEMBL
        if drug in parser.drug_to_chembl:
            #print("ChEMBL of drug {}: {}".format(drug, parser.drug_to_chembl[drug]))
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "CHEMBL", value=parser.drug_to_chembl[drug], type="cross-reference") )
        else:
            #print("ChEMBL not available for {}".format(drug))
            pass

        # Associate its ChEBI
        if drug in parser.drug_to_chebi:
            #print("ChEBI of drug {}: {}".format(drug, parser.drug_to_chebi[drug]))
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "CHEBI", value=parser.drug_to_chebi[drug], type="cross-reference") )
        else:
            #print("ChEBI not available for {}".format(drug))
            pass

        # Associate its InChIKey
        if drug in parser.drug_to_inchi_key:
            #print("InChIKey of drug {}: {}".format(drug, parser.drug_to_inchi_key[drug]))
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "InChIKey", value=parser.drug_to_inchi_key[drug], type="unique") )
        else:
            #print("InChIKey not available for {}".format(drug))
            pass

        # Associate its SMILES
        if drug in parser.drug_to_smiles:
            #print("SMILES of drug {}: {}".format(drug, parser.drug_to_smiles[drug]))
            new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "SMILES", value=parser.drug_to_smiles[drug], type="unique") )
        else:
            #print("SMILES not available for {}".format(drug))
            pass

        # Associate its ATC codes
        if drug in parser.drug_to_atc_codes:
            for atc in parser.drug_to_atc_codes[drug]:
                new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "ATC", value=atc.upper(), type="cross-reference") )
        else:
            #print("ATC codes not available for {}".format(drug))
            pass

        # Insert this external entity into database
        self.external_entity_ids_dict[drug] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

        # Associate its targets

        #### drug_to_target_to_values: drug => target_name => [ type of target (target, enzyme, transporter...) , known action (yes = True, no = False) , actions (inhibitor, substrate, antagonist...) ]

        if drug in parser.drug_to_target_to_values:

            for target in parser.drug_to_target_to_values[drug]:

                type_of_target = parser.drug_to_target_to_values[drug][target][0].split('}')[1].lower()
                if type_of_target == "target":
                    type_of_target = "therapeutic"

                target_type_string = '{}---{}'.format(target, type_of_target)
                if not self.external_entity_ids_dict.has_key(target_type_string):
                    target_external_entity = ExternalEntity( source_database = self.database, type = "protein" )

                    target_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "DrugBank_targetID", value=target.upper(), type="unique",
                                                                                   additional_fields = {"targetType": type_of_target} ) )

                    if target in parser.target_to_uniprot:
                        target_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotAccession", value=parser.target_to_uniprot[target], type="cross-reference") )
                        #print("Target Uniprot: {}".format(parser.target_to_uniprot[target]))
                    if target in parser.target_to_uniprotentry:
                        target_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "UniprotEntry", value=parser.target_to_uniprotentry[target], type="cross-reference") )
                        #print("Target Uniprot Entry: {}".format(parser.target_to_uniprotentry[target]))
                    if target in parser.target_to_gene:
                        target_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "GeneSymbol", value=parser.target_to_gene[target], type="cross-reference") )
                        #print("Target Gene: {}".format(parser.target_to_gene[target]))

                    self.external_entity_ids_dict[target_type_string] = self.biana_access.insert_new_external_entity( externalEntity = target_external_entity )

                # Create an external entity relation corresponding to interaction between Uniprot_id1 and Uniprot_id2 in database
                new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "interaction" )
                # Associate drug as the first participant in this interaction
                new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[drug] )
                # Associate ddi as the second participant in this interaction
                new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[target_type_string] )

                # Get known action
                known_action = parser.drug_to_target_to_values[drug][target][1]
                if not known_action:
                    known_action = 'unknown'

                # Get actions
                actions = parser.drug_to_target_to_values[drug][target][2]

                # Associate the drug-target interaction with the known action
                new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier= "DrugBank_known_action", value=known_action, type="unique") )

                # Associate the drug-target interaction with the actions
                if len(actions) > 0:
                    for action in actions:
                        new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier= "DrugBank_action", value=action.lower(), type="unique") )

                # Insert this external entity realtion into database
                self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )


        else:
            #print("Targets not available for {}".format(drug))
            pass


        return new_external_entity



class DrugBankXMLParser(object):
    NS="{http://www.drugbank.ca}"

    def __init__(self, filename):
        self.file_name = filename
        self.drugs = set()
        self.drug_to_name = {}
        self.drug_to_description = {}
        self.drug_to_type = {}
        self.drug_to_groups = {}
        self.drug_to_indication = {}
        self.drug_to_pharmacodynamics = {}
        self.drug_to_moa = {}
        self.drug_to_toxicity = {}
        self.drug_to_synonyms = {}
        self.drug_to_products = {}
        self.drug_to_brands = {}
        self.drug_to_mixtures = {}
        self.mixture_to_ingredients = {}
        self.drug_to_uniprot = {}
        self.drug_to_interactions = {} 
        self.drug_to_pubchem = {}
        self.drug_to_pubchem_substance = {}
        self.drug_to_chembl = {}
        self.drug_to_chebi = {}
        self.drug_to_kegg = {}
        self.drug_to_kegg_compound = {}
        self.drug_to_pharmgkb = {}
        self.drug_to_target_to_values = {} # drug - target - (type {target / enzyme / transporter / carrier}, known action, [action types])
        self.drug_to_categories = {}
        self.drug_to_atc_codes = {}
        self.drug_to_inchi_key = {}
        self.drug_to_smiles = {}
        self.target_to_name = {}
        self.target_to_gene = {}
        self.target_to_uniprot = {}
        self.target_to_uniprotentry = {}
        return

    def parse(self):
        # get an iterable
        context = iterparse(self.file_name, ["start", "end"])
        # turn it into an iterator
        context = iter(context)
        # get the root element
        event, root = context.next()
        state_stack = [ root.tag ]
        drug_id = None
        drug_type = None
        drug_id_partner = None
        current_target = None
        resource = None
        current_property = None 
        target_types = set(map(lambda x: self.NS+x, ["target", "enzyme", "carrier", "transporter"]))
        target_types_plural = set(map(lambda x: x+"s", target_types))
        for (event, elem) in context:
            if event == "start":
                state_stack.append(elem.tag)
                if len(state_stack) <= 2 and elem.tag == self.NS+"drug":
                    if "type" in elem.attrib:
                        drug_type = elem.attrib["type"]
                    else:
                        drug_type = None
                elif elem.tag == self.NS+"drugbank-id": 
                    if "primary" in elem.attrib and state_stack[-3] == self.NS+"drugbank" and state_stack[-2] == self.NS+"drug":
                        drug_id = None
                    elif len(state_stack) > 3 and state_stack[-3] == self.NS+"drug-interactions" and state_stack[-2] == self.NS+"drug-interaction":
                        drug_id_partner = None
                elif elem.tag == self.NS+"resource":
                    resource = None
                elif elem.tag == self.NS+"property":
                    current_property = None
                elif elem.tag in target_types: 
                    if state_stack[-2] in target_types_plural: 
                        current_target = None 
            if event == "end":
                if len(state_stack) <= 2 and elem.tag == self.NS+"drug":
                    if "type" in elem.attrib:
                        drug_type = elem.attrib["type"]
                    else: 
                        drug_type = None
                if elem.tag == self.NS+"drugbank-id":
                    if state_stack[-2] == self.NS+"drug":
                        if "primary" in elem.attrib:
                            drug_id = elem.text
                            self.drugs.add(drug_id.upper())
                            if drug_type is not None: 
                                self.drug_to_type[drug_id] = drug_type
                            #print drug_id, drug_type
                    elif len(state_stack) > 3 and state_stack[-3] == self.NS+"drug-interactions" and state_stack[-2] == self.NS+"drug-interaction":
                        self.drug_to_interactions.setdefault(drug_id, {})
                        drug_id_partner = elem.text
                        if drug_id_partner not in self.drug_to_interactions[drug_id]:
                            self.drug_to_interactions[drug_id][drug_id_partner] = []
                elif elem.tag == self.NS+"name":
                    if len(state_stack) <= 3 and state_stack[-2] == self.NS+"drug": 
                        self.drug_to_name[drug_id] = elem.text.strip()
                    elif state_stack[-2] == self.NS+"product" and state_stack[-3] == self.NS+"products":
                        product = elem.text
                        product = product.strip().encode('ascii','ignore')
                        if product != "":
                            self.drug_to_products.setdefault(drug_id, set()).add(product)
                    elif state_stack[-2] == self.NS+"international-brand" and state_stack[-3] == self.NS+"international-brands":
                        brand = elem.text
                        #idx = brand.find(" [")
                        #if idx != -1:
                        #    brand = brand[:idx]
                        brand = brand.strip().encode('ascii','ignore')
                        if brand != "":
                            self.drug_to_brands.setdefault(drug_id, set()).add(brand) 
                    #elif state_stack[-3] == self.NS+"targets" and state_stack[-2] == self.NS+"target":
                    elif state_stack[-2] == self.NS+"mixture" and state_stack[-3] == self.NS+"mixtures":
                        mixture = elem.text
                        mixture = mixture.strip().encode('ascii','ignore')
                        if mixture != "":
                            self.drug_to_mixtures.setdefault(drug_id, set()).add(mixture)
                    elif state_stack[-3] in target_types_plural and state_stack[-2] in target_types:
                        self.target_to_name[current_target] = elem.text 
                elif elem.tag == self.NS+"ingredients":
                    if state_stack[-3] == self.NS+"mixtures" and state_stack[-2] == self.NS+"mixture":
                        ingredients = elem.text 
                        ingredients = ingredients.strip().encode('ascii','ignore')
                        if ingredients != "" and mixture != "":
                            self.mixture_to_ingredients[mixture] = ingredients
                elif elem.tag == self.NS+"description":
                    if state_stack[-2] == self.NS+"drug":
                        self.drug_to_description[drug_id] = elem.text
                    if len(state_stack) > 3 and state_stack[-3] == self.NS+"drug-interactions" and state_stack[-2] == self.NS+"drug-interaction":
                        self.drug_to_interactions[drug_id][drug_id_partner].append(elem.text)
                elif elem.tag == self.NS+"group":
                    if state_stack[-2] == self.NS+"groups":
                        self.drug_to_groups.setdefault(drug_id, set()).add(elem.text)
                elif elem.tag == self.NS+"indication":
                    if state_stack[-2] == self.NS+"drug":
                        self.drug_to_indication.setdefault(drug_id, [])
                        self.drug_to_indication[drug_id].append(elem.text)
                elif elem.tag == self.NS+"pharmacodynamics":
                    if state_stack[-2] == self.NS+"drug":
                        self.drug_to_pharmacodynamics[drug_id] = elem.text
                elif elem.tag == self.NS+"mechanism-of-action":
                    if state_stack[-2] == self.NS+"drug":
                        self.drug_to_moa[drug_id] = elem.text
                elif elem.tag == self.NS+"toxicity":
                    if state_stack[-2] == self.NS+"drug":
                        self.drug_to_toxicity[drug_id] = elem.text
                elif elem.tag == self.NS+"synonym":
                    if state_stack[-2] == self.NS+"synonyms" and state_stack[-3] == self.NS+"drug":
                        synonym = elem.text
                        idx = synonym.find(" [")
                        if idx != -1:
                            synonym = synonym[:idx]
                        synonym = synonym.strip().encode('ascii','ignore')
                        if synonym != "":
                            self.drug_to_synonyms.setdefault(drug_id, set()).add(synonym) 
                elif elem.tag == self.NS+"category":
                    if state_stack[-2] == self.NS+"categories":
                        self.drug_to_categories.setdefault(drug_id, set()).add(elem.text)
                elif elem.tag == self.NS+"atc-code":
                    if state_stack[-2] == self.NS+"atc-codes":
                        self.drug_to_atc_codes.setdefault(drug_id, set()).add(elem.attrib["code"])
                elif elem.tag == self.NS+"id":        
                    if state_stack[-3] in target_types_plural and state_stack[-2] in target_types:
                        current_target = elem.text
                        self.drug_to_target_to_values.setdefault(drug_id, {})
                        self.drug_to_target_to_values[drug_id][current_target] = [state_stack[-2], False, []]
                        #print current_target 
                elif elem.tag == self.NS+"action":        
                    if state_stack[-3] in target_types and state_stack[-2] == self.NS+"actions":
                        self.drug_to_target_to_values[drug_id][current_target][2].append(elem.text)
                elif elem.tag == self.NS+"known-action":
                    if state_stack[-2] in target_types:
                        if elem.text == "yes":
                            self.drug_to_target_to_values[drug_id][current_target][1] = True
                            if len(self.drug_to_target_to_values[drug_id][current_target][2]) == 0:
                                #print "Inconsistency with target action: {} {}".format(drug_id, current_target)
                                pass
                elif elem.tag == self.NS+"gene-name":
                    if state_stack[-3] in target_types and state_stack[-2] == self.NS+"polypeptide":
                        self.target_to_gene[current_target] = elem.text 
                elif elem.tag == self.NS+"kind":
                    if state_stack[-3] == self.NS+"calculated-properties" and state_stack[-2] == self.NS+"property":
                        current_property = elem.text # InChIKey or SMILES
                elif elem.tag == self.NS+"value":
                    if state_stack[-3] == self.NS+"calculated-properties" and state_stack[-2] == self.NS+"property":
                        if current_property == "InChIKey":
                            inchi_key = elem.text # strip InChIKey=
                            if inchi_key.startswith("InChIKey="):
                                inchi_key = inchi_key[len("InChIKey="):]
                            self.drug_to_inchi_key[drug_id] = inchi_key
                        if current_property == "SMILES":
                            self.drug_to_smiles[drug_id] = elem.text 
                elif elem.tag == self.NS+"resource":
                    if state_stack[-3] == self.NS+"external-identifiers" and state_stack[-2] == self.NS+"external-identifier":
                        resource = elem.text 
                elif elem.tag == self.NS+"identifier":
                    if state_stack[-3] == self.NS+"external-identifiers" and state_stack[-2] == self.NS+"external-identifier":
                        if state_stack[-5] in target_types and state_stack[-4] == self.NS+"polypeptide":
                            if resource == "UniProtKB":
                                self.target_to_uniprot[current_target] = elem.text
                            if resource == "UniProt Accession":
                                self.target_to_uniprotentry[current_target] = elem.text
                        elif state_stack[-4] == self.NS+"drug":
                            if resource == "PubChem Compound":
                                self.drug_to_pubchem[drug_id] = elem.text
                            elif resource == "PubChem Substance":
                                self.drug_to_pubchem_substance[drug_id] = elem.text
                            elif resource == "ChEBI":
                                self.drug_to_chebi[drug_id] = elem.text
                            elif resource == "ChEMBL":
                                self.drug_to_chembl[drug_id] = elem.text
                            elif resource == "KEGG Drug":
                                self.drug_to_kegg[drug_id] = elem.text
                            elif resource == "KEGG Compound":
                                self.drug_to_kegg_compound[drug_id] = elem.text
                            elif resource == "UniProtKB":
                                self.drug_to_uniprot[drug_id] = elem.text
                            elif resource == "PharmGKB":
                                self.drug_to_pharmgkb[drug_id] = elem.text
                elem.clear()
                state_stack.pop()
        root.clear()
        return 

    
    def get_targets(self, target_types = set(["target"]), only_paction=False):
        # Map target ids to uniprot ids
        target_types = map(lambda x: self.NS + x, target_types)
        drug_to_uniprots = {}
        for drug, target_to_values in self.drug_to_target_to_values.iteritems():
            for target, values in target_to_values.iteritems():
                #print target, values
                try:
                    uniprot = self.target_to_uniprot[target]
                except:
                    # drug target has no uniprot
                    #print "No uniprot information for", target 
                    continue
                target_type, known, actions = values
                flag = False
                if only_paction:
                    if known:
                        flag = True
                else:
                    if target_type in target_types:
                        flag = True
                if flag:
                    drug_to_uniprots.setdefault(drug, set()).add(uniprot)
        return drug_to_uniprots


    def get_synonyms(self, selected_drugs=None, only_synonyms=False):
        name_to_drug = {}
        for drug, name in self.drug_to_name.iteritems():
            if selected_drugs is not None and drug not in selected_drugs:
                continue
            name_to_drug[name.lower()] = drug
        synonym_to_drug = {}
        for drug, synonyms in self.drug_to_synonyms.iteritems():
            for synonym in synonyms:
                if selected_drugs is not None and drug not in selected_drugs:
                    continue
                synonym_to_drug[synonym.lower()] = drug
        if only_synonyms:
            return name_to_drug, synonym_to_drug
        for drug, brands in self.drug_to_brands.iteritems():
            for brand in brands:
                if selected_drugs is not None and drug not in selected_drugs:
                    continue
                synonym_to_drug[brand.lower()] = drug
        for drug, products in self.drug_to_products.iteritems():
            for product in products:
                if selected_drugs is not None and drug not in selected_drugs:
                    continue
                synonym_to_drug[product.lower()] = drug
        return name_to_drug, synonym_to_drug


    def get_drugs_by_group(self, groups_to_include = set(["approved"]), groups_to_exclude=set(["withdrawn"])):
        selected_drugs = set()
        for drugbank_id, name in self.drug_to_name.iteritems():
            # Consider only approved drugs
            if drugbank_id not in self.drug_to_groups:
                continue
            groups = self.drug_to_groups[drugbank_id]
            #if "approved" not in groups or "withdrawn" in groups: 
            if len(groups & groups_to_include) == 0:
                continue
            if len(groups & groups_to_exclude) > 0:
                continue
            selected_drugs.add(drugbank_id)
        return selected_drugs


def output_data(file_name, out_file):
    dump_file = file_name + ".pcl"
    if os.path.exists(dump_file):
        parser = cPickle.load(open(dump_file))
    else:
        parser = DrugBankXMLParser(file_name)
        parser.parse()
        cPickle.dump(parser, open(dump_file, 'w'))
    #target_type_list = ["target", "enzyme", "carrier", "transporter"]
    #for target_type in target_type_list:
    target_type_list = ["target"]
    drug_to_uniprots = parser.get_targets(target_types = set(target_type_list), only_paction=False)
    f = open(out_file, 'w')
    f.write("Drugbank id\tName\tGroup\tTargets\n") 
    #f.write("Drugbank id\tName\tGroup\tTarget uniprots\tEnzyme uniprots\tTransporter uniprots\tCarrier uniprots\tDescription\tIndication\tPubChem\tSMILES\tInchi\tAlternative names\t\n")
    #drug_to_description drug_to_indication drug_to_synonyms drug_to_products drug_to_brands 
    for drug, uniprots in drug_to_uniprots.iteritems():
        name = parser.drug_to_name[drug]
        groups = parser.drug_to_groups[drug]
        values = [ drug, name.encode("ascii", "replace") ]
        values.append(" | ".join(groups))
        values.append(" | ".join(uniprots))
        try:
            f.write("{}\n".format("\t".join(values)))
        except: 
            print values
    f.close()
    return


if __name__ == "__main__":
    main()
