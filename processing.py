import subprocess
import os
import uuid
try:
    from osgeo import ogr
except Exception as e:
    print(e)
import sql
# from threading import Thread, current_thread
import concurrent.futures
import time
import utilities


class footprint:
    def info():
        print('Generating Footprints')

    def generate(datadir, tempdir):
        rows = sql.select("SELECT * FROM footprintmissing", '')
        # https://stackoverflow.com/a/15143994/1623867
        # ThreadPoolExecutor for I/O bound operations
        # ProcessPoolExecutor for CPU bound
        multicore = False
        start = time.time()
        if multicore is True:
            executor = concurrent.futures.ProcessPoolExecutor(1)
            futures = [executor.submit(
                footprint.processor, row, datadir, tempdir
            ) for row in rows]
            concurrent.futures.wait(futures)
        else:
            for row in rows:
                footprint.processor(row, datadir, tempdir)
        end = time.time()
        print(end - start)

    def try_my_operation(row, datadir, tempdir):
        try:
            print('ID: {ID} {row} {dd} {td}'.format(
                ID=os.getpid(), row=row, dd=datadir, td=tempdir
            ))
        except:
            print('error with item')

    def processor(row, datadir, tempdir):
        print('INFO: Process ID: {ID} {row} {dd} {td}'.format(
            ID=os.getpid(),
            row=row,
            dd=datadir,
            td=tempdir
        ))
        orderNumber = str(row[0])
        filename = str(row[1])
        noaaid = str(row[2])

        workingdir = os.path.join(tempdir, 'temp')
        if not os.path.isdir(workingdir):
            os.mkdir(os.path.expanduser(workingdir))

        file = os.path.join(datadir, orderNumber, filename)
        if not os.path.isfile(file):
            print('WARNING: File {f} is not there'.format(f=file))
        else:
            print(workingdir, orderNumber, file, filename, noaaid)

            out = footprint.extract(workingdir, orderNumber, file)
            # This will load the shape to the database if the file exists
            if os.path.exists(out):
                footprint.loadgeomtopgsql(out, filename, noaaid, orderNumber)
        utilities.filesandfolders.deletefiles(workingdir)

    def extract(workingdir, orderNumber, file):
        layer = '//All_Data/VIIRS-DNB-SDR_All/Radiance'

        # gdal_translate -of GTiff HDF5:"D:\TEMP\noaa2\GDNBO-SVDNB_npp_d20171125_t2100535_e2106339_b31503_c20171128024404585872_nobc_ops.h5"://All_Data/VIIRS-DNB-SDR_All/Radiance test.tif
        infile = 'HDF5:"{file}":{layer}'.format(file=file, layer=layer)
        r1 = '{u}{end}'.format(u=str(uuid.uuid4()), end='.tif')
        outfile = os.path.join(workingdir, r1)  # this should be different from basedir
        gdaltranslate = '{tool} {of} {infile} {outfile}'.format(
            tool='gdal_translate',
            of='-of GTiff',
            infile=infile,
            outfile=outfile
        )
        print(gdaltranslate)
        subprocess.check_call(gdaltranslate)

        # gdalwarp -dstnodata 0 -dstalpha -of GTiff test.tif test2.tif
        infile = os.path.join(workingdir, r1)
        r2 = '{u}{end}'.format(u=str(uuid.uuid4()), end='.tif')
        outfile = os.path.join(workingdir, r2)
        gdalwarp = '{tool} {param} {of} {infile} {outfile}'.format(
            tool='gdalwarp',
            param='-ot Int16 -wt Int16 -dstnodata 0 -dstalpha',
            of='-of GTiff',
            infile=infile,
            outfile=outfile
        )
        print(gdalwarp)
        subprocess.check_call(gdalwarp)

        # gdal_polygonize.py -q test2.tif -b 2 -f "ESRI Shapefile" testshape.shp
        infile = os.path.join(workingdir, r2)
        r3 = '{u}{end}'.format(u=str(uuid.uuid4()), end='.shp')
        outfile = os.path.join(workingdir, r3)
        polygonize = '{tool} {param1} {infile} {param2} {outfile}'.format(
            tool='gdal_polygonize',
            param1='-8 -q',
            infile=infile,
            param2='-b 2 -f "ESRI Shapefile"',
            outfile=outfile
        )

        print(polygonize)
        #  shell=True is very unsecure we have to fix this!
        subprocess.check_call(polygonize, shell=True)

        # simplify
        # ogr2ogr output.shp input.shp -simplify 0.0001
        infile = os.path.join(workingdir, r3)
        outfile = os.path.join(workingdir, '{u}{end}'.format(
            u=str(uuid.uuid4()),
            end='.shp')
        )
        simplify = '{tool} {outfile} {infile} {param}'.format(
            tool='ogr2ogr',
            outfile=outfile,
            infile=infile,
            param='-simplify 0.2'
        )
        print(simplify)
        subprocess.check_call(simplify)

        return outfile

    def loadgeomtopgsql(file, filename, noaaid, orderNumber):
        reader = ogr.Open(file)
        layer = reader.GetLayer(0)
        try:
            epsg = layer.GetSpatialRef().GetAuthorityCode("GEOGCS")
        except:
            print('Cannot find EPSG code')

        for i in range(layer.GetFeatureCount()):
            feature = layer.GetFeature(i)
            # print(feature.ExportToJson())
            geom = feature.geometry()
            SQL = "INSERT INTO {table}(file_name, noaaid, orderNumber, footprint) SELECT '{fn}', {ni}, {on}, ST_GeomFromText('{geom}',{epsg})".format(
                table='imagedata',
                fn=filename,
                ni=noaaid,
                on=orderNumber,
                geom=str(geom),
                epsg=str(epsg)
            )
            data = ('',)
            try:
                sql.insert(SQL,data)
            except Exception as e:
                raise
            else:
                pass
            finally:
                pass

            # INSERT INTO imagedata(footprint) SELECT ST_GeomFromText('POLYGON ((47.8986402364685 -19.9761359737374,77.2019166143206 -24.5331415521829,75.348830485111 -44.4051468911004,38.8567335982238 -38.6872585624496,47.8986402364685 -19.9761359737374))',4326)