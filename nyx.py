# FROM https://www.tutorialspoint.com/postgresql/postgresql_python.htm
# FROM http://www.tutorialspoint.com/python/python_command_line_arguments.htm

import configparser
import argparse
import sys, os, getopt
import psycopg2
import pycurl
from io import StringIO,BytesIO
import xml.etree.ElementTree as ET
from datetime import datetime

config = configparser.ConfigParser()
config_file = os.path.join(os.path.dirname(__file__), 'settings.cfg')
config.read(config_file)
cfg_database = config['PostgreSQL']['database']
cfg_user = config['PostgreSQL']['user']
cfg_password = config['PostgreSQL']['password']
cfg_host = config['PostgreSQL']['host']
cfg_port = config['PostgreSQL']['port']
cfg_path = config['Server']['path']

class sql:
	def connect(self):
		try:
			connection = psycopg2.connect(database=cfg_database, user = cfg_user, password = cfg_password, host = cfg_host, port = cfg_port)
		except psycopg2.OperationalError as e:
			print('ERROR: Cannot connect to database')
			print('{message}'.format(message=str(e)))
			exit()
		cursor = connection.cursor()
		return connection,cursor

	def disconnect(self,connection,cursor):
		cursor.close()
		connection.close()

	def select(self,s,d):
		conn, cur = self.connect()
		cur.execute(s,d)
		rows = cur.fetchall()
		conn.commit()
		self.disconnect(conn, cur)
		return rows

	def insert(self,s,d):
		conn, cur = self.connect()
		try:
			cur.execute(s,d)
		except psycopg2.Error as e:
			print('ERROR: {message}'.format(message=str(e)))
			exit()
		try:
			res = conn.commit()
			return res
		except psycopg2.Error as e:
			print('ERROR: {message}'.format(message=str(e)))
			exit()
		self.disconnect(conn, cur)

	def update(self,s,d):
		conn, cur = self.connect()
		cur.execute(s,d)
		conn.commit()
		self.disconnect(conn, cur)

	def delete(self,s,d):
		conn, cur = self.connect()
		cur.execute(s,d)
		conn.commit()
		self.disconnect(conn, cur)

	def selectordertable(self):
		SQL = "SELECT user_id, ordernumber, status from orders ORDER by user_id"
		data = ('',)
		r = self.select(SQL,data) #returns rows
		return r

	def selectorders(self):
		SQL = "SELECT user_id, ordernumber, status from orders WHERE status = %s ORDER by user_id"
		data = ('NEW',)
		r = self.select(SQL,data) #returns rows
		return r

	def selectprogresstable(self):
		SQL = "SELECT * FROM public.progresstable;"
		data = ('',)
		r = self.select(SQL,data) #returns rows
		return r

	def printprogresstable(self,rows):
		print('|{i: >12}'.format(i='OrderNumber'),
			  '|{i: >15}'.format(i='Status'),
			  '|{i: >4}'.format(i='N'),
			  '|{i: >4}'.format(i='D'),
			  '|{i: >4}'.format(i='T'),
			  '|{i: >8}'.format(i='S [GB]'),
			  '|{i: <70}|'.format(i='Destination'))

		print('-----------------------------------------------------------------------------------------------------------------------------------')
		for row in rows:
			c1 = '|{i: >{width}}'.format(i=row[0],width=12)
			c2 = '|{i: ^{width}}'.format(i=row[1],width=15)
			c3 = '|{i: >{width}}'.format(i=row[2],width=4)
			c4 = '|{i: >{width}}'.format(i=row[3],width=4)
			c5 = '|{i: >{width}}'.format(i=row[4],width=4)
			if row[5] is None:
				c6 = '|{0: >#08.2f}'. format(float(0))
			else:
				c6 = '|{0: >#08.2f}'. format(float(row[5]))
			if row[6] is None:
				c7 = '|{i: <{width}}|'.format(i='',width=70)
			else:
				c7 = '|{i: <{width}}|'.format(i=row[6],width=70)
			print(c1,c2,c3,c4,c5,c6,c7)


	def printtable(self,rows):
		print("ID \t OrderNumber \t Status")
		for row in rows:
			print(row[0],"\t",row[1],"\t",row[2])

	def setOrderStatus(self,o,s):
		SQL = "UPDATE orders set status = %s where ordernumber = %s"
		data = (s,o)
		self.update(SQL,data)

class ftp:
	def dirlist(self,u):
		# lets create a buffer in which we will write the output
		buffer = BytesIO()

		# lets create a pycurl object
		c = pycurl.Curl()
		
		# lets specify the details of FTP server
		#c.setopt(pycurl.URL, r'ftp://ftp.ncbi.nih.gov/refseq/release/')
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
		return body

	def file(self, u, o):
		with open(o, 'wb') as f:
			c = pycurl.Curl()
			c.setopt(c.URL, u)
			c.setopt(c.WRITEDATA, f)
			c.setopt(c.NOPROGRESS,0)
			print('downloading file {f}'.format(f = os.path.basename(o)))
			try:
				c.perform()
			except:
				code = c.getinfo(pycurl.HTTP_CODE)
				print('ERROR: cURL HTTP_CODE:', code)
			c.close()

class manifest():
	def download(self,u,p,o):
		sql_c = sql()
		manifestname = self.getName(u)
		if manifestname == '':
			print('ERROR: There seems to be no Manifest file for order {number}'.format(number=o))
			sql_c.setOrderStatus(o,'NOMANIFEST')
		else:
			u += manifestname
			p = os.path.join(p,manifestname)
			ftp_c = ftp()
			sql_c = sql()
			ftp_c.file(u,p)
			if os.path.exists(p): #also check file size here
				SQL = "UPDATE orders set manifest = %s WHERE ordernumber = %s"
				data = (manifestname,o)
				sql_c.update(SQL,data)
				sql_c.setOrderStatus(o,'MANIFEST')
			else:
				print('ERROR: There is no Manifest for order %s',o)

	def getName(self,u): #url, location, ordernumber, path
		ftp_c = ftp()
		result = ftp_c.dirlist(u)
		# lets print the string on screen
		# print(result.decode('iso-8859-1'))
		# FTP LIST buffer is separated by \r\n
		# lets split the buffer in lines
		lines = result.decode('iso-8859-1').split('\r\n')
		counter = 0
		suffix = '.xml'
		manifestname = ''
		# lets walk through each line
		for line in lines:
		    counter += 1
		    if counter > (len(lines)-1):
		    	break
		    parts = line.split()
		    #print(parts[8]) # * is the filename of the directory listing
		    if parts[8].endswith(suffix):
		    	manifestname = parts[8]
		return manifestname

	def process(self):
		sql_c = sql()
		SQL = ("SELECT * FROM processmanifest")
		data = ('',)
		rows = sql_c.select(SQL,data)
		print('INFO: Processing Manifest for',len(rows),'orders with the status MANIFEST')

		for row in rows:
			orderNumber = row[0]
			path = row[1]
			manifest = row[2]
			if os.path.exists(os.path.join(path, str(orderNumber), manifest)):
				if self.loadxml(os.path.join(path,str(orderNumber),manifest),orderNumber) == 1:
					sql_c.setOrderStatus(str(orderNumber),'READY')
				else:
					sql_c.setOrderStatus(str(orderNumber),'ERROR')
		exit()

	def loadxml(self,xmlfile,orderNumber):
		print('INFO: Loading XML Manifest file', str(xmlfile),'into table images')
		tree = ET.parse(xmlfile)
		root = tree.getroot()
		sql_c = sql()
		comment = root.find('comment').text
		total_files = int(root.find('total_files').text)
		print(comment, total_files)
		counter = 1
		for lineitem in root.findall('./line_item'):
			noaaid = lineitem.get('id')
			file_name = lineitem.find('item/file_name').text
			file_size = lineitem.find('item/file_size').text
			creation_date = lineitem.find('item/creation_date').text
			creation_date = datetime.strptime(creation_date, "%Y-%m-%dT%H:%M:%SZ")
			expiration_date = lineitem.find('item/expiration_date').text
			expiration_date =  datetime.strptime(expiration_date, "%Y-%m-%dT%H:%M:%SZ")
			try:
				checksum = lineitem.find('item/checksum').text
			except:
				print('ERROR: Manifest at',str(xmlfile),'does not include checksum')
				checksum = None
			print('INFO: Loading data to database table images:',noaaid, file_name, file_size, creation_date, expiration_date, checksum)
			#SQL = "INSERT INTO images (manifest,file_name,checksum,ordernumber,ordercreated,orderexpiration,status,file_size) VALUES (%s,%s,%s,%s,TIMESTAMP %s,TIMESTAMP %s,%s,%s);" # Note: no quotes
			#data = (os.path.basename(xmlfile),file_name,checksum,orderNumber,creation_date,expiration_date,'NEW',file_size)
			#sql_c.insert(SQL,data) #NEEDS to be wrapped in a try statement
			counter += 1
		if total_files == counter:
			return 1
		else:
			return 0

class utils:
	def deletefiles(self,dir):
		filelist = [ f for f in os.listdir(dir) ]
		for f in filelist:
			os.remove(os.path.join(dir, f))

	def deletefolder(self,dir):
		os.rmdir(dir)

	def query_yes_no(self,question, default='yes'):
	    """Ask a yes/no question via raw_input() and return their answer.
	    
	    "question" is a string that is presented to the user.
	    "default" is the presumed answer if the user just hits <Enter>.
	        It must be "yes" (the default), "no" or None (meaning
	        an answer is required of the user).

	    The "answer" return value is one of "yes" or "no".
	    """
	    # from http://code.activestate.com/recipes/577058-query-yesno/
	    valid = {'yes':'yes',   'y':'yes',  'ye':'yes',
	             'no':'no',     'n':'no'}
	    if default == None:
	        prompt = ' [y/n] '
	    elif default == 'yes':
	        prompt = ' [Y/n] '
	    elif default == 'no':
	        prompt = ' [y/N] '
	    else:
	        raise ValueError('invalid default answer: {d}'.format(d=default))

	    while 1:
	        sys.stdout.write(question + prompt)
	        choice = input().lower()
	        if default is not None and choice == '':
	            return default
	        elif choice in valid.keys():
	            return valid[choice]
	        else:
	            sys.stdout.write('Please respond with \'yes\' or \'no\' (or \'y\' or \'n\').')

class image:
	def download(self):
		s = sql()
		f = ftp()
		print('actually downloading images')
		SQL = ("select * from downloadimages")
		data = ('',)
		rows = s.select(SQL,data)
		for row in rows:
				orderNumber = row[0]
				filename = row[1]
				destination = row[2]
				server = row[3]
				checksum = row[4]
				dest = os.path.join(destination,str(orderNumber),str(filename))
				url = 'ftp://ftp.class.{s}.noaa.gov/{o}/001/{f}'.format(s=server,o=orderNumber,f=filename)
				print('Downloading',filename,'from', url)
				res = f.file(str(url),str(dest))
				print(res)
		return 'done'

class order:
	def add(self, orderNumber, server, directory):
		SQL = "INSERT INTO orders (ordernumber, status, server,directory) VALUES (%s,%s,%s,%s);" # Note: no quotes
		data = (orderNumber, "NEW", server, directory)
		s = sql()
		r = s.insert(SQL,data)
		return r

	def remove(self,o):
		s = sql()
		SQL = "SELECT * FROM deleteorder WHERE ordernumber = %s"
		data = (o,)
		rows = s.select(SQL,data)
		for row in rows:
			orderNumber = row[0]
			notice = row[1]
			status = row[2]
			directory = row[3]
			folder = os.path.join(directory,str(orderNumber))
			print('Order', orderNumber,'(',notice, ') has the status', status)
			question = 'Are you sure you want to delete this order at {d} ?'.format(d=folder)
			decision = query_yes_no(question,  default="yes")
			if decision == 'yes' and os.path.exists(folder):
				self.deletefiles(folder)
				self.deletefolder(folder)
				s.setOrderStatus(row[0],'DELETED')
			else:
				print('Nothing to delete.')
		exit()

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
		utils_c = utils()
		if p == '':
			question = 'Should the data be stored at the default path {p}'.format(p=cfg_path)
			answer = utils_c.query_yes_no(question, default='yes')
			if answer == 'yes':
				return cfg_path
			else:
				print('Provide a directory like "-p /var/www/vhosts/geoinsight.xyz/noaa.geoinsight.xyz/NOAA"')
				exit()
		else:
			return p

def create_arg_parser():
	""""Creates and returns the ArgumentParser object."""
	#https://stackoverflow.com/questions/14360389/getting-file-path-from-command-line-argument-in-python
	parser = argparse.ArgumentParser(description='This program manages orders form NOAA CLASS')
	parser.add_argument('-m','--mode',default="list", choices = ['list','addOrder','getManifest','processManifest','downloadImages','deleteOrder'],
					help='What do you want to do?')
	parser.add_argument('-o', '--orderNumber',default="",
					help='The Order Number from NOAA CLASS')
	parser.add_argument('-s', '--status',default="",
					help='The Status of the order')	
	parser.add_argument('-l', '--server',default="", choices = ['ncdc','ngdc'],
					help='The location of the order')	
	parser.add_argument('-p',  '--path',default="",
					help='Path to the output directory')
	return parser

def main(argv):
	sql_c = sql()
	manifest_c = manifest()
	image_c = image()
	order_c = order()

	arg_parser = create_arg_parser()
	parsed_args = arg_parser.parse_args(sys.argv[1:])
	mode = parsed_args.mode

	if mode == 'list':
		print('Current progress table')
		sql_c.printprogresstable(sql_c.selectprogresstable())
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
		rows = sql_c.select(SQL,data)
		sql_c = sql()
		for row in rows:
			orderNumber = str(row[0])			
			server = str(row[1])
			path = str(row[2])
			url = 'ftp://ftp.class.{s}.noaa.gov/{o}/'.format(s=server,o=orderNumber)
			destination = os.path.join(path,orderNumber)
			if not os.path.isdir(path):
				sql_c.setOrderStatus(orderNumber,'CHECKPATH')
				print('This path does not exist on this server')
				continue
			else:
				if not os.path.isdir(destination):
					os.mkdir(os.path.expanduser(destination))
				manifest_c.download(url,destination,orderNumber)

	elif mode == 'processManifest':
		manifest_c.process()
	elif mode == 'downloadImages':
		i.download()
	elif mode == 'deleteOrder':
		orderNumber = checkInput.orderNumber(parsed_args.orderNumber)
		order_c.delete(orderNumber)

	exit()

if __name__ == "__main__":
	main(sys.argv[1:])