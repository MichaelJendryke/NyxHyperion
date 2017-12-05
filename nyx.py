# FROM https://www.tutorialspoint.com/postgresql/postgresql_python.htm
# FROM http://www.tutorialspoint.com/python/python_command_line_arguments.htm

import configparser
import argparse
import sys
import os
import pycurl
from io import BytesIO
import xml.etree.ElementTree as ET
from datetime import datetime
import processing as proc
import sql
import utilities

config = configparser.ConfigParser()
config_file = os.path.join(os.path.dirname(__file__), 'settings.cfg')
config.read(config_file)
cfg_path = config['Server']['path']
cfg_limit = int(config['Server']['limit'])


class image:
    def download(self):
        ftp_c = ftp()
        SQL = "SELECT * FROM downloadimages"  # all that are not finished
        data = ('', )
        rows = sql.select(SQL, data)
        print('INFO: {d} images to download'.format(d=len(rows)))
        for row in rows:
            orderNumber = row[0]
            filename = row[1]
            destination = row[2]
            server = row[3]
            checksum = row[4]
            filesize = row[5]
            if not utilities.freespace(destination):
                print('ERROR: Not enough space on server the limit is {limit}GB'.format(limit=cfg_limit))
                exit()  # this will exit the whole program
            else:
                print('INFO: Still enough space, limit is {limit}GB'.format(limit=cfg_limit))

            # actually downloading the file
            dest = os.path.join(destination, str(orderNumber), str(filename))
            url = 'ftp://ftp.class.{s}.noaa.gov/{o}/001/{f}'.format(s=server, o=orderNumber, f=filename)
            ftpres = ftp_c.file(str(url), str(dest))

            if ftpres is None:
                print('INFO: Download completed')
            else:
                print('INFO: Finished with Error {r}'.format(r=ftpres))
                sql.setImageStatus(orderNumber, filename, 'ERROR')
                continue  # continiue with next row

            if (
                    # check in the database if the checksum was given, if not, it is non-verified download
                    (not checksum == '') and
                    (self.checksumcheck(dest, checksum.replace('-', ''))) and
                    (utilities.getFileSize(dest) == filesize)
            ):
                print('INFO: Download size and md5 verified')
                sql.setImageStatus(orderNumber, filename, 'FINISHED')
            else:
                print('ERROR: Download of {d} has errors'.format(d=dest))
                sql.setImageStatus(orderNumber, filename, 'ERROR')

            if sql.ordercomplete(orderNumber) is True:
                sql.setOrderStatus(orderNumber, 'FINISHED')

    def checksumcheck(self, d, c):
        filechecksum = utilities.md5sum(d)
        xmlchecksum = c
        if filechecksum == xmlchecksum:
            return 1
        else:
            return 0


class order:
    def add(self, orderNumber, server, directory):
        SQL = "INSERT INTO orders (ordernumber, status, server,directory) VALUES (%s,%s,%s,%s);"  # Note: no quotes
        data = (orderNumber, "NEW", server, directory)
        r = sql.insert(SQL, data)
        return r

    def remove(self, o):
        SQL = "SELECT * FROM deleteorder WHERE ordernumber = %s"
        data = (o,)
        rows = sql.select(SQL, data)
        for row in rows:
            orderNumber = row[0]
            notice = row[1]
            status = row[2]
            directory = row[3]
            folder = os.path.join(directory, str(orderNumber))
            print('Order {order} [{notice}] has the status {status}'.format(
                order=orderNumber, notice=notice, status=status))
            question = 'Are you sure you want to delete this order at {d}?'.format(d=folder)
            decision = utilities.query_yes_no(question, default="yes")
            if decision == 'yes':
                utilities.deletefiles(folder)
                utilities.deletefolder(folder)
                if not os.path.exists(folder):
                    sql.setOrderStatus(orderNumber, 'DELETED')
            elif decision == 'no':
                print('Nothing will be delete.')
            else:
                print('ERROR: {d} not a local folder.'.format(d=folder))
                print('INFO: Are you on the right computer?')
        exit()


class manifest():
    def download(self, u, p, o):
        manifestname = self.getName(u)
        if manifestname == '':
            print('ERROR: There seems to be no Manifest file for order {number}'.format(number=o))
            sql.setOrderStatus(o, 'NOMANIFEST')
        else:
            u += manifestname
            p = os.path.join(p, manifestname)
            ftp_c = ftp()
            ftp_c.file(u, p)
            if os.path.exists(p):  # also check file size here
                SQL = "UPDATE orders set manifest = %s WHERE ordernumber = %s"
                data = (manifestname, o)
                sql.update(SQL, data)
                sql.setOrderStatus(o, 'MANIFEST')
            else:
                print('ERROR: There is no Manifest for order {o}'.format(o=o))

    def getName(self, u):  # url, location, ordernumber, path
        ftp_c = ftp()
        result = ftp_c.dirlist(u)
        # lets print the string on screen
        # print(result.decode('iso-8859-1'))
        # FTP LIST buffer is separated by \r\n
        # lets split the buffer in lines
        if not result.decode('iso-8859-1').find('\r\n') == -1:
            lines = result.decode('iso-8859-1').split('\r\n')
        else:
            lines = result.decode('iso-8859-1').replace('\n', '\r\n').split('\r\n')
        counter = 0
        suffix = '.xml'
        manifestname = ''
        # lets walk through each line
        for line in lines:
            counter += 1
            if counter > (len(lines) - 1):
                break
            parts = line.split()
            # print(parts[8])  # * is the filename of the directory listing
            if parts[8].endswith(suffix):
                manifestname = parts[8]
        return manifestname

    def process(self):
        SQL = ("SELECT * FROM processmanifest")
        data = ('',)
        rows = sql.select(SQL, data)
        print('INFO: Processing Manifest for', len(rows), 'orders with the status MANIFEST')
        for row in rows:
            orderNumber = row[0]
            path = row[1]
            manifest = row[2]
            if os.path.exists(os.path.join(path, str(orderNumber), manifest)):
                if self.loadxml(os.path.join(path, str(orderNumber), manifest), orderNumber) == 1:
                    sql.setOrderStatus(str(orderNumber), 'READY')
                else:
                    sql.setOrderStatus(str(orderNumber), 'ERROR')
        exit()

    def loadxml(self, xmlfile, orderNumber):
        print('INFO: Loading XML Manifest file', str(xmlfile), 'into table images')
        tree = ET.parse(xmlfile)
        root = tree.getroot()
        comment = root.find('comment').text
        total_files = int(root.find('total_files').text)
        for lineitem in root.findall('./line_item'):
            try:
                noaaid = lineitem.get('id')
                file_name = lineitem.find('item/file_name').text
                file_size = lineitem.find('item/file_size').text
                creation_date = lineitem.find('item/creation_date').text
                creation_date = datetime.strptime(creation_date, "%Y-%m-%dT%H:%M:%SZ")
                expiration_date = lineitem.find('item/expiration_date').text
                expiration_date = datetime.strptime(expiration_date, "%Y-%m-%dT%H:%M:%SZ")
            except:
                print('ERROR: Cannot read all values in Manifest', str(xmlfile))

            try:
                checksum = lineitem.find('item/checksum').text
            except:
                print('ERROR: Manifest at', str(xmlfile), 'does not include checksum')
                checksum = None

            print('INFO: Loading data to database table images:\
                ', noaaid, file_name, file_size, creation_date, expiration_date, checksum)
            SQL = "INSERT INTO images (manifest,file_name,checksum,ordernumber,ordercreated,orderexpiration,status,file_size,noaaid) VALUES (%s,%s,%s,%s,TIMESTAMP %s,TIMESTAMP %s,%s,%s,%s);" # Note: no quotes
            data = (os.path.basename(xmlfile), file_name, checksum, orderNumber, creation_date, expiration_date, 'NEW', file_size, noaaid)
            try:
                sql.insert(SQL, data)
                print('insert')
            except:
                print('ERROR: Information for this image and order already present?')

        SQL = "UPDATE orders set notice = %s, manifesttotal = %s WHERE ordernumber = %s"
        data = (comment, total_files, orderNumber)
        try:
            sql.insert(SQL, data)
        except:
            print('ERROR: Cannot insert in database')

        SQL = "SELECT COUNT(ordernumber) FROM images WHERE ordernumber = %s"
        data = (orderNumber,)
        count = sql.select(SQL, data)
        if total_files == count[0][0]:  # get the only element that the query returns
            return 1
        else:
            return 0


class ftp:
    def dirlist(self, u):
        # lets create a buffer in which we will write the output
        buffer = BytesIO()

        # lets create a pycurl object
        c = pycurl.Curl()

        # lets specify the details of FTP server
        # c.setopt(pycurl.URL, r'ftp://ftp.ncbi.nih.gov/refseq/release/')
        c.setopt(pycurl.URL, u)

        # lets assign this buffer to pycurl object
        c.setopt(pycurl.WRITEFUNCTION, buffer.write)

        # lets perform the LIST operation
        try:
            c.perform()
        except:
            code = c.getinfo(pycurl.HTTP_CODE)
            print('ERROR: cURL HTTP_CODE:', code)
        c.close()

        # lets get the buffer in a string
        body = buffer.getvalue()
        print(body)
        return body  # Returns Bytes!!!

    def file(self, u, o):
        with open(o, 'wb') as f:
            c = pycurl.Curl()
            c.setopt(c.URL, u)
            c.setopt(c.WRITEDATA, f)
            c.setopt(c.NOPROGRESS, 1)  # Show (0) or not show (1) the progress
            print('INFO: Downloading file from {u} to {o}'.format(o=o, u=u))
            try:
                c.perform()
            except:
                code = c.getinfo(pycurl.HTTP_CODE)
                print('ERROR: cURL HTTP_CODE:', code)
            c.close()


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
            print('Provide a valid ORDERNUMBER like "-o 12344256"')
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
            print('ERROR: Provide a directory like "-d /home/mydata" \
                (this should point to the directory with all your order folders')
            exit()
        elif not os.path.exists(dir):
            print('ERROR: Data directory {d} does not exist'.format(d=dir))
            exit()
        else:
            return dir

    def workingdir(dir):
        if dir == '':
            print('ERROR: Provide a temporary working directory like "-w /tmp" \
                (this should point to the directory outside of datadir')
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
        choices=['info', 'list', 'addOrder', 'getManifest', 'processManifest','downloadImages', 'deleteOrder', 'generateFootprint'],
        help='What do you want to do?'
    )
    parser.add_argument(
        '-v', '--view',
        default="overview",
        choices = ['overview', 'imagesummary', 'orders', 'images'],
        help='Print a table or view'
    )
    parser.add_argument(
        '-o', '--orderNumber',
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
        default="",
        choices = ['ncdc', 'ngdc'],
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
    manifest_c = manifest()
    image_c = image()
    order_c = order()

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
        orderNumber = checkInput.orderNumber(parsed_args.orderNumber)
        server = checkInput.server(parsed_args.server)
        directory = checkInput.path(parsed_args.path)
        if order_c.add(orderNumber, server, directory) is None:
            print('Order added')
        else:
            print('There was an error, the order has not been added')
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
                manifest_c.download(url, destination, orderNumber)

    elif mode == 'processManifest':
        manifest_c.process()
    elif mode == 'downloadImages':
        image_c.download()
    elif mode == 'deleteOrder':
        orderNumber = checkInput.orderNumber(parsed_args.orderNumber)
        order_c.remove(orderNumber)
    elif mode == 'generateFootprint':
        print('Do some processing')
        datadir = checkInput.datadir(parsed_args.datadir)
        workingdir = checkInput.workingdir(parsed_args.workingdir)
        proc.footprint.info()
        proc.footprint.generate(datadir, workingdir)

    exit()


if __name__ == "__main__":
    main(sys.argv[1:])