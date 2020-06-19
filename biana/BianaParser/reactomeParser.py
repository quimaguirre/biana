from bianaParser import *
                    
class reactomeParser(BianaParser):                                                        
    """             
    MyData Parser Class 

    Parses data from the Reactome interactome

    """                 
                                                                                         
    name = "reactome"
    description = "This file implements a program that fills up tables in BIANA database from data in the Reactome"
    external_entity_definition = "An external entity represents a protein"
    external_entity_relations = "An external relation represents a interaction"
            
    def __init__(self):
        """
        Start with the default values
        """
        BianaParser.__init__(self, default_db_description = "Reactome pathways and reactions",  
                             default_script_name = "reactomeParser.py",
                             default_script_description = reactomeParser.description,     
                             additional_compulsory_arguments = [])
        self.default_eE_attribute = "reactome"
                    
    def parse_database(self):
        """                                                                              
        Method that implements the specific operations of a MyData formatted file
        """

        #-------------------#
        # CHECK INPUT FILES #
        #-------------------#

        # Check that all the files exist
        if os.path.isdir(self.input_file):
            self.input_path = self.input_file
        else:
            raise ValueError("You must specify a directory instead of a file.")

        onlyfiles = [f for f in os.listdir(self.input_path) if os.path.isfile(os.path.join(self.input_path, f))]
        for input_file in onlyfiles:
            if input_file.startswith('NCBI2Reactome_All_Levels'):
                all_levels_file = os.path.join(self.input_path, input_file)
            elif input_file.startswith('NCBI2ReactomeReactions'):
                reactions_file = os.path.join(self.input_path, input_file)
            elif input_file.startswith('NCBI2Reactome_PE_Pathway'):
                pe_all_levels_file = os.path.join(self.input_path, input_file)
            elif input_file.startswith('NCBI2Reactome_PE_Reactions'):
                pe_reactions_file = os.path.join(self.input_path, input_file)
            elif input_file.startswith('ReactomePathways'):
                reactome_pathways_file = os.path.join(self.input_path, input_file)


        #----------------#
        # PARSE DATABASE #
        #----------------#

        parser = Reactome(all_levels_file, reactions_file, pe_all_levels_file, pe_reactions_file, reactome_pathways_file)
        parser.parse()


        #---------------------------------#
        # INSERT PARSED DATABASE IN BIANA #
        #---------------------------------#




class Reactome(object):

    def __init__(self, all_levels_file, reactions_file, pe_all_levels_file, pe_reactions_file, reactome_pathways_file):

        self.all_levels_file = all_levels_file
        self.reactions_file = reactions_file
        self.pe_all_levels_file = pe_all_levels_file
        self.pe_reactions_file = pe_reactions_file
        self.reactome_pathways_file = reactome_pathways_file

        return

    def parse(self):

        return

if __name__ == "__main__":
    main()

