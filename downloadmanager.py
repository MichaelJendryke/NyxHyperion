''' Manages Downloads from NOAA CLASS
    - manifest
    - orders
    - images '''

import xml.etree.ElementTree as ET
from datetime import datetime
import ftp
import sql
import utilities
import configparser
import os
from tabulate import tabulate

config = configparser.ConfigParser()
config_file = os.path.join(os.path.dirname(__file__), 'settings.cfg')
config.read(config_file)
cfg_path = config['Server']['path']
cfg_limit = int(config['Server']['limit'])


class image:
    def download():
        SQL = "SELECT * FROM downloadimages"  # all that are READY
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
            if not utilities.filesandfolders.freespace(destination):
                print('ERROR: Not enough space on server the limit is {limit}GB'.format(
                    limit=cfg_limit)
                )
                exit()  # this will exit the whole program
            else:
                print('INFO: Still enough space, limit is {limit}GB'.format(limit=cfg_limit))

            # actually downloading the file
            dest = os.path.join(destination, str(orderNumber), str(filename))
            url = 'ftp://ftp.class.{s}.noaa.gov/{o}/001/{f}'.format(
                s=server,
                o=orderNumber,
                f=filename
            )
            ftpres = ftp.file(str(url), str(dest))

            if ftpres is None:
                print('INFO: Download completed')
            else:
                print('INFO: Finished with Error {r}'.format(r=ftpres))
                sql.setImageStatus(orderNumber, filename, 'ERROR')
                continue  # continiue with next row

            if (
                    # check in the database if the checksum was given, if not, it is non-verified download
                    (not checksum == '') and
                    (image.checksumcheck(dest, checksum.replace('-', ''))) and
                    (utilities.filesandfolders.getFileSize(dest) == filesize)
            ):
                print('INFO: Download size and md5 verified')
                sql.setImageStatus(orderNumber, filename, 'FINISHED')
            else:
                print('ERROR: Download of {d} has errors'.format(d=dest))
                sql.setImageStatus(orderNumber, filename, 'ERROR')

            if sql.ordercomplete(orderNumber) is True:
                sql.setOrderStatus(orderNumber, 'FINISHED')

    def checksumcheck(d, c):
        filechecksum = utilities.filesandfolders.md5sum(d)
        xmlchecksum = c
        if filechecksum == xmlchecksum:
            return 1
        else:
            return 0


class order:
    def add(orderNumber, server, directory):
        # Note: no quotes
        SQL = "INSERT INTO orders (ordernumber, status, server,directory) VALUES (%s,%s,%s,%s);"
        data = (orderNumber, "NEW", server, directory)
        r = sql.insert(SQL, data)
        return r

    def integrityCheck(baseDir):
        """Checks the integrity of the local files against the Database"""
        print('Check Folder, Files and DB integrity')
        SQL = "SELECT ordernumber FROM orders WHERE status <> 'CHECKED'"
        data = ('',)
        orderRows = sql.select(SQL, data)
        if orderRows == []:
            print('INFO: Nothing to check, all good')
            exit()

        print('INFO: The following {x} orders have to be checked'.format(
            x=len(orderRows)))
        print(tabulate(orderRows))
        for row in orderRows:
            orderNumber = str(row[0])
            orderPath = os.path.join(baseDir, orderNumber)
            if not os.path.exists(orderPath):
                print('WARNING: {f} does not even exist, continiue'.format(
                    f=orderPath
                ))
                continue
            else:
                print('INFO: Now checking order {o}'.format(
                    o=orderNumber
                ))
            SQL = "SELECT file_name, checksum, file_size FROM images WHERE orderNumber = %s"
            data = (str(orderNumber),)
            frows = sql.select(SQL, data)
            print('INFO: Checking {x} files for order {o}'.format(
                x=len(frows),
                o=orderNumber
            ))
            counter = 0
            for f in frows:
                counter += 1
                dbFileName = f[0]
                dbFileChkS = f[1].replace('-', '')
                dbFileSize = f[2]
                fileToCheck = os.path.join(baseDir, orderNumber, dbFileName)
                if os.path.isfile(fileToCheck):
                    c = utilities.filesandfolders.md5sum(fileToCheck)
                    s = utilities.filesandfolders.getFileSize(fileToCheck)
                    if c == dbFileChkS and s == dbFileSize:
                        print('INFO: {x: >3}/{y} {f} is CHECKED'.format(
                            f=fileToCheck,
                            x=counter,
                            y=len(frows)
                        ))
                        sql.setImageStatus(
                            orderNumber,
                            dbFileName,
                            'CHECKED'
                        )
                    else:
                        print('ERROR: {x: >3}/{y} {f} seems BROKEN'.format(
                            f=fileToCheck,
                            x=counter,
                            y=len(frows)
                        ))
                        sql.setImageStatus(
                            orderNumber,
                            dbFileName,
                            'BROKEN'
                        )
                else:
                    print('WARNING: File {f} does not exist locally'.format(
                        f=fileToCheck
                    ))
            sql.orderChecked(orderNumber)
        print('All done')
        exit()

    def remove():
        """REMOVES all order folders without question"""
        SQL = "SELECT * FROM deleteorder WHERE status = 'CHECKED'"
        data = ('',)
        rows = sql.select(SQL, data)
        for row in rows:
            orderNumber = row[0]
            notice = row[1]
            status = row[2]
            directory = row[3]
            folder = os.path.join(directory, str(orderNumber))

            if os.path.isdir(folder):
                print('Order {order} [{notice}] with status {status} deleted'.format(
                    order=orderNumber,
                    notice=notice,
                    status=status)
                )
                utilities.filesandfolders.deletefiles(folder)
                utilities.filesandfolders.deletefolder(folder)
            else:
                print('ERROR: {d} not a local folder.'.format(d=folder))
                print('INFO: Are you on the right computer?')
        exit()


class manifest():
    def download(u, p, o):
        manifestname = manifest.getName(u)
        if manifestname == '':
            print('ERROR: There seems to be no Manifest file for order {number}'.format(number=o))
            sql.setOrderStatus(o, 'NOMANIFEST')
        else:
            u += manifestname
            p = os.path.join(p, manifestname)
            ftp.file(u, p)
            if os.path.exists(p):  # also check file size here
                SQL = "UPDATE orders set manifest = %s WHERE ordernumber = %s"
                data = (manifestname, o)
                sql.update(SQL, data)
                sql.setOrderStatus(o, 'MANIFEST')
            else:
                print('ERROR: There is no Manifest for order {o}'.format(o=o))

    def getName(u):  # url, location, ordernumber, path
        result = ftp.dirlist(u)
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

    def process():
        SQL = ("SELECT * FROM processmanifest")
        data = ('',)
        rows = sql.select(SQL, data)
        print('INFO: Processing Manifest for', len(rows), 'orders with the status MANIFEST')
        for row in rows:
            orderNumber = row[0]
            path = row[1]
            manifestfile = str(row[2])
            if os.path.exists(os.path.join(path, str(orderNumber), manifestfile)):
                if manifest.loadxml(os.path.join(path, str(orderNumber), manifestfile), orderNumber) == 1:
                    sql.setOrderStatus(str(orderNumber), 'READY')
                else:
                    sql.setOrderStatus(str(orderNumber), 'ERROR')
        exit()

    def loadxml(xmlfile, orderNumber):
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
