import os
from hashlib import md5

class filesandfolders:
	def deletefiles(dir):
		if os.path.exists(dir):
			filelist = [ f for f in os.listdir(dir) ]
			for f in filelist:
				byebye = os.path.join(dir, f)
				if os.path.isfile(byebye):
					os.remove(byebye)
				else:
					print('ERROR: {d} is not a local path'.format(d = byebye))

	def deletefolder(dir):
		if os.path.exists(dir):
			os.rmdir(dir)
		else:
			print('ERROR: {d} is not a local path'.format(d = dir))

	def freespace(d):
	foldersize = int(math.floor(getFolderSize(d)/1024**3)) 
	if foldersize > (cfg_limit -1): # -1 to be sure to be under the limit 
		r = 0
	else:
		r = 1
	return r

	def getFolderSize(d):
		total_size = 0
		for dirpath, dirnames, filenames in os.walk(d):
			for f in filenames:
				fp = os.path.join(dirpath, f)
				total_size += os.path.getsize(fp)
		 return total_size

	def getFileSize(d):
		try:
			b = os.path.getsize(d)
			return b
		except:
			print('file {file} does not exist or is inaccessible'.format(file = d))


	def md5sum(filename): # https://bitbucket.org/prologic/tools/src/tip/md5sum?fileviewer=file-view-default
		hash = md5()
		with open(filename, "rb") as f:
			for chunk in iter(lambda: f.read(128 * hash.block_size), b""):
				hash.update(chunk)
		return hash.hexdigest()

class queries:
	def query_yes_no(question, default='yes'):
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


