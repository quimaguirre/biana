"""
    BIANA: Biologic Interactions and Network Analysis
    Copyright (C) 2009  Javier Garcia-Garcia, Emre Guney, Baldo Oliva

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""

import sys

import re

import csv

from bianaParser import *


class ATCParser(BianaParser):
    """
    Anatomical Therapeutic Chemical Classification Parser Class
    """

    name = "atc"
    description = "This program fills up tables in database biana related with ATC classification"
    external_entity_definition = "A external entity represents an ontology element"
    external_entity_relations = ""


    def __init__(self):
        
        BianaParser.__init__(self, default_db_description = "Anatomical Therapeutic Chemical Classification",
                             default_script_name = "ATCParser.py",
                             default_script_description = "This program fills up tables in database biana related to OBO 1.2 formatted Ontologies")
        self.default_eE_attribute = "atc"
        

    def parse_database(self):
        """
        Method that implements the specific operations of Anatomical Therapeutic Chemical Classification parser
        """


        # Add a different type of external entity
        self.biana_access.add_valid_external_entity_type( type = "ATCElement" )

        # Add the attributes specific of ATC database
        self.biana_access.add_valid_external_entity_attribute_type( name = "ATC",
                                                                    data_type = "varchar(10)",
                                                                    category = "eE attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "ATC_name",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE identifier attribute")

        self.biana_access.add_valid_external_entity_attribute_type( name = "ATC_level",
                                                                    data_type = "varchar(1)",
                                                                    category = "eE attribute")

        # IMPORTANT: As we have added new types and attributes that are not in the default BIANA distribution, we must execute the follwing command:
        self.biana_access.refresh_database_information()

        

        # Add the possibility to transfer ATC name using ATC term as a key
        self.biana_access._add_transfer_attribute( externalDatabaseID = self.database.get_id(), 
                                                   key_attribute = "ATC",
                                                   transfer_attribute="ATC_name" )

        ontology = Ontology( source_database = self.database, linkedAttribute="ATC", name="ATC", descriptionAttribute="ATC_name" )

        specific_identifiers_and_parent = {}

        # Start variables
        term_id = None
        term_name = None
        term_def = None

        term_namespace = None 
        #term_synonyms = []
        term_is_a = []
        term_part_of = []
        term_exact_synonyms = []
        term_related_synonyms = []
        term_broad_synonyms = []
        term_narrow_synonyms = []
        term_alt_id = []

        self.initialize_input_file_descriptor()

        # # Adding a dummy relation to let the database know that go_name attribute is also a possible relation attribute
        # externalEntityRelation = ExternalEntityRelation( source_database = self.database, relation_type = "interaction" )
        # externalEntityRelation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "go_name", value = None) )

	def create_external_entity_from_atc_term(database, atc, atc_name, atc_level,
		term_namespace, term_def, term_exact_synonyms, term_related_synonyms, term_broad_synonyms, term_narrow_synonyms, term_alt_id):

	    externalEntity = ExternalEntity( source_database = database, type = "ATCElement" )
	    
	    externalEntity.add_attribute( ExternalEntityAttribute( attribute_identifier = "ATC", value = atc, type="unique") )

	    if atc_name is not None:
		externalEntity.add_attribute( ExternalEntityAttribute( attribute_identifier = "ATC_name", value = atc_name, type="unique") )

            if atc_level is not None:
                externalEntity.add_attribute( ExternalEntityAttribute( attribute_identifier = "ATC_level", value = atc_level, type="unique") )

	    if term_def is not None:
		externalEntity.add_attribute( ExternalEntityAttribute( attribute_identifier = "description", value = term_def) )
	    
	    for current_synonym in term_exact_synonyms:
		if current_synonym is not None:
		    externalEntity.add_attribute( ExternalEntityAttribute( attribute_identifier = "ATC_name",
									   value = current_synonym,
									   type = "exact_synonym" ) )
                    
	    for current_synonym in term_related_synonyms:
		if current_synonym is not None:
		    externalEntity.add_attribute( ExternalEntityAttribute( attribute_identifier = "ATC_name",
		    						       value = current_synonym,
		    						       type = "related_synonym" ) )
                    
	    for current_synonym in term_broad_synonyms:
		if current_synonym is not None:
		    externalEntity.add_attribute( ExternalEntityAttribute( attribute_identifier = "ATC_name",
		    						       value = current_synonym,
		    						       type = "broad_synonym" ) )
                    
	    for current_synonym in term_narrow_synonyms:
		if current_synonym is not None:
		    externalEntity.add_attribute( ExternalEntityAttribute( attribute_identifier = "ATC_name",
		    						       value = current_synonym,
		    						       type = "narrow_synonym" ) )
                    
        # Quim Aguirre: Adding the alternative ATC ids as "alias" in ATC table
	    for current_alt_id in term_alt_id:
		if current_alt_id is not None:
		    externalEntity.add_attribute( ExternalEntityAttribute( attribute_identifier = "ATC",
		    						       value = current_alt_id,
		    						       type = "alias" ) )
	    return externalEntity


        # Variables:
        atc_ids = set()
        atc2name = {}
        atc2parents = {}
        atc2isa = {}
        atc2level = {}


        input_file_fd = open(self.input_file,'r')

        input_file_fd.readline()

        data = csv.reader(input_file_fd, delimiter=',')

        #Class ID,Preferred Label,Synonyms,Definitions,Obsolete,CUI,Semantic Types,Parents,ATC LEVEL,Has member,Inverse of isa,Is Drug Class,isa,Member of,Semantic type UMLS property
        for fields in data:

            atc = fields[0]
            name = fields[1]
            parents = fields[7]
            level = fields[8]
            has_member = fields[9]
            isa = fields[12]
            member_of = fields[13]

            # Obtain ATC id from 'http://purl.bioontology.org/ontology/UATC/L04AA31'
            atc_split = atc.split('/')
            atc = atc_split[len(atc_split)-1]
            db = atc_split[len(atc_split)-2]
            if db != 'UATC':
                continue
            atc_ids.add(atc)

            # Add the name
            if atc not in atc2name:
                atc2name[atc] = name
            else:
                print('ATC {} two times in atc2name'.format(atc))
                sys.exit(10)

            atc2parents.setdefault(atc, [])

            # Add the 'PartOf' elements from 'parents' or 'member_of'
            if parents != '' and member_of == '':
                parents = parents.split('|')
                for parent in parents:
                    parent_split = parent.split('/')
                    parent_name = parent_split[len(parent_split)-1]
                    if parent_name == 'owl#Thing':
                        continue
                    atc2parents[atc].append(parent_name)

            elif parents == '' and member_of != '':
                member_of = member_of.split('|')
                for member in member_of:
                    member_split = member.split('/')
                    member_name = member_split[len(member_split)-1]
                    if member_name == 'owl#Thing':
                        continue
                    atc2parents[atc].append(member_name)
            elif parents != '' and member_of != '':
                print('{} PARENTS: {}'.format(atc, parents))
                print('{} MEMBER OF: {}'.format(atc, member_of))
                print('TWO PARENTS')
                sys.exit(10)
            elif parents == '' and member_of == '':
                pass

            atc2isa.setdefault(atc, [])
            if isa != '':
                isa = isa.split('|')
                for isa_id in isa:
                    isa_split = isa_id.split('/')
                    isa_name = isa_split[len(isa_split)-1]
                    atc2isa[atc].append(isa_name)


            if level != '':
                atc2level[atc] = int(level)
            else:
                print('NO LEVEL IN ATC {}'.format(atc))
                sys.exit(10)


        # Insert all the external Entities
        for atc in atc_ids:
            externalEntity = create_external_entity_from_atc_term(self.database, atc, atc2name[atc], atc2level[atc],
               term_namespace, term_def, term_exact_synonyms, 
               term_related_synonyms, term_broad_synonyms, term_narrow_synonyms, term_alt_id)
            self.biana_access.insert_new_external_entity( externalEntity )
            specific_identifiers_and_parent[atc] = (externalEntity.get_id(), atc2isa[atc], atc2parents[atc])

        # Set the ontology hierarch and insert elements to ontology
        for current_method_id in specific_identifiers_and_parent:
            #print(current_method_id)
            #print(specific_identifiers_and_parent[current_method_id])
            is_a_list = [ specific_identifiers_and_parent[x][0] for x in specific_identifiers_and_parent[current_method_id][1] ]
            is_part_of_list = [ specific_identifiers_and_parent[x][0] for x in specific_identifiers_and_parent[current_method_id][2]  ]
            ontology.add_element( ontologyElementID = specific_identifiers_and_parent[current_method_id][0],
                                  isA = is_a_list,
                                  isPartOf = is_part_of_list )

        self.biana_access.insert_new_external_entity(ontology)




    def obtain_header_fields(self, first_line, separator='\t'):
        """ 
        Obtain a dictionary: "field_name" => "position" 
        """
        fields_dict = {}

        header_fields = first_line.strip().split(separator)
        for x in xrange(0, len(header_fields)):
            fields_dict[header_fields[x]] = x

        return fields_dict

