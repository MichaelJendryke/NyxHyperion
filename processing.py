import subprocess
import os
import uuid
try:
	from osgeo import ogr
except Exception as e:
	print(e)
import sql


class footprint:
	def info():
			print('Generating Footprints')

	def generate(datadir,tempdir):
		rows = sql.select("SELECT * FROM footprintmissing",'')
		print('INFO: {num} images have no Footprint in table imagedata'.format(num = len(rows)))
		for row in rows:
			orderNumber = str(row[0])
			filename = str(row[1])
			
			workingdir = os.path.join(tempdir,'temp')
			if not os.path.isdir(workingdir):
				os.mkdir(os.path.expanduser(workingdir))

			file = os.path.join(datadir,orderNumber,filename)
			if not os.path.isfile(file):
				print('WARNING: File {f} is not there'.format(f = file))
			else:
				print(datadir,workingdir,orderNumber, file)
				out = footprint.extract(datadir,workingdir,orderNumber,file)
				if os.path.exists(out):
					footprint.loadgeomtopgsql(out)
					
	def extract(basedir, workingdir,orderNumber,file):
		layer = '//All_Data/VIIRS-DNB-SDR_All/Radiance'

		#gdal_translate -of GTiff HDF5:"D:\TEMP\noaa2\GDNBO-SVDNB_npp_d20171125_t2100535_e2106339_b31503_c20171128024404585872_nobc_ops.h5"://All_Data/VIIRS-DNB-SDR_All/Radiance test.tif
		infile = 'HDF5:"{file}":{layer}'.format(file = file,layer=layer)
		r1 = '{u}{end}'.format(u = str(uuid.uuid4()), end = '.tif')
		outfile = os.path.join(workingdir,r1) # this should be different from basedir
		gdaltranslate = '{tool} {of} {infile} {outfile}'.format(tool = 'gdal_translate', of = '-of GTiff', infile = infile, outfile = outfile)
		print(gdaltranslate)
		subprocess.check_call(gdaltranslate)
		
		#gdalwarp -dstnodata 0 -dstalpha -of GTiff test.tif test2.tif
		infile = os.path.join(workingdir,r1)
		r2 = '{u}{end}'.format(u = str(uuid.uuid4()), end = '.tif')
		outfile = os.path.join(workingdir,r2)
		gdalwarp = '{tool} {param} {of} {infile} {outfile}'.format(tool = 'gdalwarp', param = '-dstnodata 0 -dstalpha', of = '-of GTiff', infile = infile, outfile = outfile)
		print(gdalwarp)
		subprocess.check_call(gdalwarp)
		
		#gdal_polygonize.py -q test2.tif -b 2 -f "ESRI Shapefile" testshape.shp
		infile = os.path.join(workingdir,r2)
		r3 = '{u}{end}'.format(u = str(uuid.uuid4()), end = '.shp')
		outfile = os.path.join(workingdir,r3)
		polygonize = '{tool} {param1} {infile} {param2} {outfile}'.format(tool = 'gdal_polygonize', param1 = '-8 -q',infile = infile,param2 = '-b 2 -f "ESRI Shapefile"', outfile = outfile)
		print(polygonize)
		subprocess.check_call(polygonize,shell=True) # shell=True is very unsecure we have to fix this!

		#simplify
		#ogr2ogr output.shp input.shp -simplify 0.0001
		infile = os.path.join(workingdir,r3)
		outfile = os.path.join(workingdir,'{u}{end}'.format(u = str(uuid.uuid4()), end = '.shp'))
		simplify = '{tool} {outfile} {infile} {param}'.format(tool = 'ogr2ogr', outfile = outfile, infile = infile,param = '-simplify 10.0')
		print(simplify)
		subprocess.check_call(simplify)

		return outfile
			


	def loadgeomtopgsql(file):
		reader = ogr.Open(file)
		layer = reader.GetLayer(0) 
		try:
			epsg = layer.GetSpatialRef().GetAuthorityCode("GEOGCS") 
		except:
			print('Cannot find EPSG code')

		for i in range(layer.GetFeatureCount()):
			feature = layer.GetFeature(i)
			#print(feature.ExportToJson())
			print(feature.geometry())
			SQL = "INSERT INTO imagedata"
			#INSERT INTO imagedata(footprint) SELECT ST_GeomFromText('POLYGON ((47.8986402364685 -19.9761359737374,77.2019166143206 -24.5331415521829,75.348830485111 -44.4051468911004,38.8567335982238 -38.6872585624496,47.8986402364685 -19.9761359737374))',4326)


