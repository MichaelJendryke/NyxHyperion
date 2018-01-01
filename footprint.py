try:
    from osgeo import ogr, gdal
    print('OK, i have osgeo')
except Exception as e:
    print(e)
import numpy as np

gdal.UseExceptions()


if not gdal.GetDriverByName('HDF5'):
    raise Exception('HDF5 driver is not available')

print('what now?')

path = "D:\\TEMP\\NOAAORDERS\\2912360305\\GDNBO-SVDNB_npp_d20140401_t2236356_e2242160_b12578_c20171130102624190714_noaa_ops.h5"

ds = gdal.Open(path)
subdataset_read = ds.GetSubDatasets()
radiance = []
for s in subdataset_read:
    if s[0].find('Radiance') > -1:
        radiance = gdal.Open(s[0], gdal.GA_ReadOnly)
        radiance = np.array(radiance.ReadAsArray())
    # print('Subdataset: {s}'.format(s=s[0]))

if radiance == []:
    exit()
else:
    print(radiance.shape)
    print('Yeah we have radiance')
    print(radiance)
    GDALPolygonize()
    