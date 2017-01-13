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

        # Add DrugCombinationID as a valid external entity attribute since it is not recognized by BIANA
        self.biana_access.add_valid_external_entity_attribute_type( name = "DrugCombinationID",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE identifier attribute")

        # Add DrugCombinationComponentID as a valid external entity attribute since it is not recognized by BIANA
        self.biana_access.add_valid_external_entity_attribute_type( name = "DrugCombinationComponentID",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE identifier attribute")

        # Add DrugCombinationTargetID as a valid external entity attribute since it is not recognized by BIANA
        self.biana_access.add_valid_external_entity_attribute_type( name = "DrugCombinationTargetID",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE identifier attribute")

        # Add ATC as a valid external entity attribute since it is not recognized by BIANA
        self.biana_access.add_valid_external_entity_attribute_type( name = "ATC",
                                                                    data_type = "varchar(370)",
                                                                    category = "eE identifier attribute")

	# Add a different type of relation type. In this case, it will be "drug combination", as it is different from a drug-drug interaction 
	self.biana_access.add_valid_external_entity_relation_type( type = "drug_combination" )

	# Since we have added new attributes that are not in the default BIANA distribution, we execute the following command
	self.biana_access.refresh_database_information()

	self.input_file_fd = open(self.input_file, 'r')
	self.external_entity_ids_dict = {}

	for line in self.input_file_fd:
		(id1, desc1, score1, id2, desc2, score2, score_int) = line.strip().split()
		# Create an external entity corresponding to Uniprot_id1 in database (if it is not already created)
		if not self.external_entity_ids_dict.has_key(id1):
			new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )
			# Annotate it as Uniprot_id1
			new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Uniprot", value=id1, type="cross-reference") )
			# Associate its description
			new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Description", value=desc1) )
			# Insert this external entity into database
			self.external_entity_ids_dict[id1] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )
		# Create an external entity corresponding to Uniprot_id2 in database (if it is not already created)
		if not self.external_entity_ids_dict.has_key(id2):
			new_external_entity = ExternalEntity( source_database = self.database, type = "protein" )
			# Annotate it as Uniprot_id2
			new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Uniprot", value=id2, type="cross-reference") )
			# Associate its description
			new_external_entity.add_attribute( ExternalEntityAttribute( attribute_identifier= "Description", value=desc2) )
			# Insert this external entity into database
			self.external_entity_ids_dict[id2] = self.biana_access.insert_new_external_entity( externalEntity = new_external_entity )

		# Create an external entity relation corresponding to interaction between Uniprot_id1 and Uniprot_id2 in database
		new_external_entity_relation = ExternalEntityRelation( source_database = self.database, relation_type = "interaction" )
		# Associate Uniprot_id1 as the first participant in this interaction
                new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[id1] )
		# Associate Uniprot_id2 as the second participant in this interaction
                new_external_entity_relation.add_participant( externalEntityID =  self.external_entity_ids_dict[values[1]] )
		# Associate score of first participant Uniprot_id1 with this interaction
		new_external_entity_relation.add_participant_attributes( externalEntityID = self.external_entity_ids_dict[id1], participantAttribute = ExternalEntityRelationParticipantAttribute( attribute_identifier = "Score", value = score1 ) )
		# Associate score of second participant Uniprot_id2 with this interaction
		new_external_entity_relation.add_participant_attributes( externalEntityID = self.external_entity_ids_dict[id2], participantAttribute = ExternalEntityRelationParticipantAttribute( attribute_identifier = "Score", value = score2 ) )
		# Associate the score of the interaction with this interaction
		new_external_entity_relation.add_attribute( ExternalEntityRelationAttribute( attribute_identifier = "AffinityScore",
                                                                                                             value = score_int ) )
		# Insert this external entity realtion into database
		self.biana_access.insert_new_external_entity( externalEntity = new_external_entity_relation )

	input_file_fd.close()

