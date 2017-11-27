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

class sql:
	database = ''
	user = ''
	password = ''
	host = ''
	port = '' 
	def config(self):
		config = configparser.ConfigParser()
		config_file = os.path.join(os.path.dirname(__file__), 'settings.cfg')
		config.read(config_file)
		self.database = config['DEFAULT']['database']
		self.user = config['DEFAULT']['user']
		self.password = config['DEFAULT']['password']
		self.host = config['DEFAULT']['host']
		self.port = config['DEFAULT']['port']

	def connect(self):
		connection = psycopg2.connect(database=self.database, user = self.user, password = self.password, host = self.host, port = self.port)
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
		cur.execute(s,d)
		conn.commit()
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
			  '|{i: <20}|'.format(i='Destination'))

		print('----------------------------------------------------------------------------------')
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
				c7 = '|{i: <{width}}|'.format(i='',width=20)
			else:
				c7 = '|{i: <{width}}|'.format(i=row[6],width=20)
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
		c.perform()

		c.close()
		
		# lets get the buffer in a string
		body = buffer.getvalue()
		return body

	def file(self, url, out):
		with open(out, 'wb') as f:
		    c = pycurl.Curl()
		    c.setopt(c.URL, url)
		    c.setopt(c.WRITEDATA, f)
		    c.setopt(c.NOPROGRESS,0)
		    try:
		    	c.perform()
		    except:
		    	print('File',url, 'not avaliable')
		    code = c.getinfo(pycurl.HTTP_CODE)
		    c.close()
		    return code
		    


class manifest():
	def getName(self,u): #url, location, ordernumber, path
		f = ftp()
		result = f.dirlist(u)
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
		if manifestname == '':
			SQL = "UPDATE orders set manifest = %s, status = %s where ordernumber = %s"
			data = ('No manifest on server','NOMANIFEST',o)
			s = sql()
			s.update(SQL,data)
		else:
			return manifestname

	def download(self,u,p,o):
		manifestname = self.getName(u)
		u += manifestname
		p = os.path.join(p,manifestname)
		f = ftp()
		s = sql()
		f.file(u,p)
		if os.path.exists(p): #also check file size here
			SQL = "UPDATE orders set manifest = %s, status = %s where ordernumber = %s"
			data = (manifestname,'MANIFEST',o)
			s.update(SQL,data)
		else:
			print('There is no Manifest for order %s',o)

	def process(self):
		s = sql()
		SQL = ("SELECT ordernumber, path,manifest FROM orders WHERE status = 'MANIFEST'")
		data = ('',)
		rows = s.select(SQL,data)
		print('INFO: Processing Manifest for',len(rows),'orders with the status MANIFEST')

		for row in rows:
			orderNumber = row[0]
			path = row[1]
			manifest = row[2]
			if not os.path.exists(os.path.join(path,str(orderNumber),manifest)):
				s.setOrderStatus(str(orderNumber),'NOMANIFEST')
			else:
				s.setOrderStatus(str(orderNumber),'LOADMANIFEST')
				try:
					self.loadxml(os.path.join(path,str(orderNumber),manifest),orderNumber)
				except:
					print(e = sys.exc_info()[0])
					s.setOrderStatus(str(orderNumber),'ERROR')
				else:
					s.setOrderStatus(str(orderNumber),'READY')
		exit()

	def loadxml(self,xmlfile,orderNumber):
		print('INFO: Loading XML Manifest file', str(xmlfile),'into table images')
		tree = ET.parse(xmlfile)
		root = tree.getroot()
		s = sql()
		for lineitem in root.findall('./line_item/item'):
			file_name = lineitem.find('file_name').text
			file_size = lineitem.find('file_size').text
			creation_date = lineitem.find('creation_date').text
			creation_date = datetime.strptime(creation_date, "%Y-%m-%dT%H:%M:%SZ")
			expiration_date = lineitem.find('expiration_date').text
			expiration_date =  datetime.strptime(expiration_date, "%Y-%m-%dT%H:%M:%SZ")
			try:
				checksum = lineitem.find('checksum').text
			except:
				print('ERROR: Manifest at',str(xmlfile),'does not include checksum')
				checksum = None
			print('INFO: Loading data to database table images:',file_name, file_size, creation_date, expiration_date, checksum)
			SQL = "INSERT INTO images (manifest,file_name,checksum,ordernumber,ordercreated,orderexpiration,status,file_size) VALUES (%s,%s,%s,%s,TIMESTAMP %s,TIMESTAMP %s,%s,%s);" # Note: no quotes
			data = (os.path.basename(xmlfile),file_name,checksum,orderNumber,creation_date,expiration_date,'NEW',file_size)
			s.insert(SQL,data) #NEEDS to be wrapped in a try statement

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


def addnewordernumber(number,loc):
	SQL = "INSERT INTO orders (ordernumber, status,location) VALUES (%s,%s,%s);" # Note: no quotes
	data = (number,"NEW",loc)
	s = sql()
	s.insert(SQL,data)

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

	def location(l):
		if l == '':
			print('Provide a valid LOCATION as per choices in "-l"')
			exit()
		else:
			return l

	def path(p):
		if not os.path.isdir(p):
			print('Provide a valid directory like "-p D:\\TEMP\\noaa"')
			exit()
		else:
			return p


def create_arg_parser():
	""""Creates and returns the ArgumentParser object."""
	#https://stackoverflow.com/questions/14360389/getting-file-path-from-command-line-argument-in-python
	parser = argparse.ArgumentParser(description='This program manages orders form NOAA CLASS')
	parser.add_argument('-m','--mode',default="list", choices = ['list','addNewOrder','getManifest','processManifest','downloadImages'],
					help='What do you want to do?')
	parser.add_argument('-o', '--orderNumber',default="",
					help='The Order Number from NOAA CLASS')
	parser.add_argument('-s', '--status',default="",
					help='The Status of the order')	
	parser.add_argument('-l', '--location',default="", choices = ['ncdc','ngdc'],
					help='The location of the order')	
	parser.add_argument('-p',  '--path',default="",
					help='Path to the output directory')
	return parser


def main(argv):
	arg_parser = create_arg_parser()
	parsed_args = arg_parser.parse_args(sys.argv[1:])

	mode = parsed_args.mode

	s = sql()
	m = manifest()
	i = image()

	if mode == 'list':
		print('Print current table')
		s = sql()
		s.config()
		s.printprogresstable(s.selectprogresstable())
	elif mode == 'addNewOrder':
		#Check other argument
		print('Add new order')
		orderNumber = checkInput.orderNumber(parsed_args.orderNumber)
		location = checkInput.location(parsed_args.location)
		addnewordernumber(orderNumber,location)
	elif mode == 'getManifest':
		#Check other argument
		path = checkInput.path(parsed_args.path)
		#NEED from database
		SQL = "SELECT location, ordernumber FROM orders WHERE status='NEW'"
		data = ('',)
		rows = s.select(SQL,data)
		for row in rows:
			location = str(row[0])
			orderNumber = str(row[1])
			SQL = "UPDATE orders set path = %s where ordernumber = %s"
			data = (path,orderNumber)
			s.update(SQL,data)			
			url = (r'ftp://ftp.class.%s.noaa.gov/%s/' % (location,orderNumber))
			destination = os.path.join(path,orderNumber)
			if not os.path.isdir(destination):
				os.mkdir(os.path.expanduser(destination))
			m.download(url,destination,orderNumber)
	elif mode == 'processManifest':
		m.process()
	elif mode == 'downloadImages':
		i.download()

	exit()


if __name__ == "__main__":
	main(sys.argv[1:])