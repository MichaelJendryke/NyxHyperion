# FROM https://www.tutorialspoint.com/postgresql/postgresql_python.htm
# FROM http://www.tutorialspoint.com/python/python_command_line_arguments.htm

import configparser
import argparse
import sys
import os
import processing as proc
import sql
import utilities
import downloadmanager

config = configparser.ConfigParser()
config_file = os.path.join(os.path.dirname(__file__), 'settings.cfg')
config.read(config_file)
cfg_path = config['Server']['path']
cfg_limit = int(config['Server']['limit'])


class checkInput:
    def orderNumber(o):
        try:
            val = int(o)
            if val < 0:  # if not a positive int print message and ask for input again
                print(o, 'is not a valid ORDERNUMBER, try again')
                exit()
            else:
                r = o
                return r
        except ValueError:
            print(o, ' is not a valid ORDERNUMBER, provide a valid ORDERNUMBER like "-o 12344256"')
            exit()

    def server(l):
        if l == '':
            print('Provide a valid LOCATION as per choices in "-l"')
            exit()
        else:
            return l

    def path(p):
        if p == '':
            question = 'Should the data be stored at the default path {p}'.format(p=cfg_path)
            answer = utilities.queries.query_yes_no(question, default='yes')
            if answer == 'yes':
                return cfg_path
            else:
                print('Provide a directory like \
                    "-p /var/www/vhosts/geoinsight.xyz/noaa.geoinsight.xyz/NOAA"')
                exit()
        else:
            return p

    def datadir(dir):
        if dir == '':
            print('ERROR:\tProvide a Data directory like "-d /home/mydata"')
            print('\t(this should point to the directory with all your order folders)')
            exit()
        elif not os.path.exists(dir):
            print('ERROR: Data directory {d} does not exist'.format(d=dir))
            exit()
        else:
            return dir

    def workingdir(dir):
        if dir == '':
            print('ERROR:\tProvide a temporary working directory like "-w /tmp"')
            print('\t(this should point to the directory outside of datadir)')
            exit()
        elif not os.path.exists(dir):
            print('ERROR: Working directory {d} does not exist'.format(d=dir))
            exit()
        else:
            return dir


def create_arg_parser():
    """"Creates and returns the ArgumentParser object."""
    # https://stackoverflow.com/questions/14360389/getting-file-path-from-command-line-argument-in-python
    parser = argparse.ArgumentParser(description='This program manages orders form NOAA CLASS')
    parser.add_argument(
        '-m', '--mode',
        default="info",
        choices=[
            'info',
            'list',
            'addOrder',
            'getManifest',
            'processManifest',
            'downloadImages',
            'deleteOrder',
            'generateFootprint'
        ],
        help='What do you want to do?'
    )
    parser.add_argument(
        '-v', '--view',
        default="overview",
        choices=[
            'overview',
            'imagesummary',
            'orders',
            'images'
        ],
        help='Print a table or view'
    )
    parser.add_argument(
        '-o', '--orderNumber',
        nargs='+',
        default="",
        help='The Order Number from NOAA CLASS'
    )
    parser.add_argument(
        '-s', '--status',
        default="",
        help='The Status of the order'
    )
    parser.add_argument(
        '-l', '--server',
        nargs='+',
        default="",
        choices=[
            'ncdc',
            'ngdc'
        ],
        help='The location of the order'
    )
    parser.add_argument(
        '-p', '--path',
        default="",
        help='Path to the output directory'
    )
    parser.add_argument(
        '-d', '--datadir',
        default="",
        help='Path to your data directory (this is used for generateFootprint)'
    )
    parser.add_argument(
        '-w', '--workingdir',
        default="",
        help='Path to your working directory (this is used for generateFootprint)'
    )
    return parser


def main(argv):
    arg_parser = create_arg_parser()
    parsed_args = arg_parser.parse_args(sys.argv[1:])
    mode = parsed_args.mode

    if mode == 'info':
        print('Allowed foldersize for {d} is {s} [GB]'.format(d=cfg_path, s=cfg_limit))
        size = utilities.filesandfolders.getFolderSize(cfg_path) / (1024**3)
        if size < cfg_limit:
            print('Currently {:6.2f} [GB] are occupied, still {:6.2f} [GB] free.'.format(
                size, (cfg_limit - size)))
        else:
            print('Folder is full. You are {:6.2f} [GB] over the limit'.format((size - cfg_limit)))
    elif mode == 'list':
        if (parsed_args.view == '') or (parsed_args.view == 'overview'):
            sql.printSQL("SELECT * FROM overview", '')
        elif (parsed_args.view == 'imagesummary'):
            sql.printSQL("SELECT * FROM imagesummary", '')
        elif (parsed_args.view == 'orders'):
            sql.printSQL("SELECT * FROM orders", '')
        elif (parsed_args.view == 'images'):
            sql.printSQL("SELECT * FROM images", '')
        else:
            print('Something went wrong')
    elif mode == 'addOrder':
        print('Add a new order')
        orderserver = zip(parsed_args.orderNumber, parsed_args.server)
        directory = checkInput.path(parsed_args.path)
        for i in orderserver:
            orderNumber = checkInput.orderNumber(i[0])
            server = checkInput.server(i[1])
            question = 'Order {o} from Server {s} will be added at {p}'.format(
                o=orderNumber,
                s=server,
                p=directory
            )
            answer = utilities.queries.query_yes_no(question)
            if answer == 'yes':
                try:
                    downloadmanager.order.add(orderNumber, server, directory)
                except:
                    print('There was an error, the order has not been added')
            else:
                print('Order will not be added.')
        exit()

    elif mode == 'getManifest':
        print('Get the manifest for NEW orders')
        SQL = "SELECT * FROM getmanifest"
        data = ('',)
        rows = sql.select(SQL, data)
        for row in rows:
            orderNumber = str(row[0])
            server = str(row[1])
            path = str(row[2])
            url = 'ftp://ftp.class.{s}.noaa.gov/{o}/'.format(s=server, o=orderNumber)
            destination = os.path.join(path, orderNumber)
            if not os.path.isdir(path):
                sql.setOrderStatus(orderNumber, 'CHECKPATH')
                print('This path does not exist on this server')
                continue
            else:
                if not os.path.isdir(destination):
                    os.mkdir(os.path.expanduser(destination))
                downloadmanager.manifest.download(url, destination, orderNumber)

    elif mode == 'processManifest':
        downloadmanager.manifest.process()
    elif mode == 'downloadImages':
        downloadmanager.image.download()
    elif mode == 'deleteOrder':
        for i in parsed_args.orderNumber:
            orderNumber = checkInput.orderNumber(i)
            downloadmanager.order.remove(orderNumber)
    elif mode == 'generateFootprint':
        print('Do some processing')
        datadir = checkInput.datadir(parsed_args.datadir)
        workingdir = checkInput.workingdir(parsed_args.workingdir)
        proc.footprint.info()
        proc.footprint.generate(datadir, workingdir)

    exit()


if __name__ == "__main__":
    main(sys.argv[1:])
