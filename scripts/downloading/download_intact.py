import sys, os, re
import optparse
import tarfile
import urllib

def main():
    """
    python /Users/quim/Dropbox/UPF/PHD/Projects/BIANA/scripts/download_intact.py -o /Users/quim/Documents/Databases/intact
    """
    options = parse_options()
    download_wget(options)
    return


def parse_options():
    """
    This function parses the command line arguments and returns an optparse object.
    """

    parser = optparse.OptionParser("download_intact.py -o OUTPUT_DIR")

    # Directory arguments
    parser.add_option("-o", action="store", type="string", dest="output_dir", help="Output folder where to store the results", metavar="OUTPUT_DIR")

    (options, args) = parser.parse_args()

    return options


def download_wget(options):
    """
    Downloads using WGET
    """
    output_dir = options.output_dir
    files_dir = os.path.join(output_dir, 'psi25')
    create_directory(output_dir)
    create_directory(files_dir)
    base_url = "ftp://anonymous:@ftp.ebi.ac.uk/pub/databases/intact/current/psi25/species"
    input_file = os.path.join(output_dir, 'files.txt')
    if not fileExist(input_file):
        #input_file = wget.download(base_url, out=input_file)
        urllib.urlretrieve(base_url, input_file)

    files_to_download = []
    with open(input_file, 'r') as input_fd:
        for line in input_fd:
            x = re.search("(\S+\.xml)", line)
            if x:
                files_to_download.append(x.group(1))

    print(files_to_download)
    for file_to_download in files_to_download:
        print(file_to_download)
        url = '{}'.format(os.path.join(base_url, file_to_download))
        output_file = os.path.join(files_dir, file_to_download)
        if not fileExist(output_file):
            urllib.urlcleanup() # Clean
            #wget.download(url, out=output_file)
            urllib.urlretrieve(url, output_file) # Retrieve
        #tar = tarfile.open(output_file, "r:gz")
        #tar.extractall(files_dir)
        #tar.close()
        #os.remove(output_file)
    return

def fileExist(file):
    """
    Checks if a file exists AND is a file
    """
    return os.path.exists(file) and os.path.isfile(file)

def create_directory(directory):
    """
    Checks if a directory exists and if not, creates it
    """
    try:
        os.stat(directory)
    except:
        os.mkdir(directory)
    return

if  __name__ == "__main__":
    main()
