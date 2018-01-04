# NyxHyperion
Processing Night-Time Light Images from VIIRS

# Requirements
* Anaconda with Python36
```bash
conda env create -n ntl -f ntl.yml
```
__Windows__ activate the environment with
```bash
activate ntl
```
__Linux__ activate the environment with
```bash
source activate ntl
```
# Order Status
* NEW
* MANIFEST
* READY
* FINISHED
* CHECKED

# Image Status
* NEW
* ERROR
* FINISHED
* BROKEN
* CHECKED
=======
# GUIDS
Add orders which are from NOAA to DATABASE_VIIRS 
```bash
python D:\NyxHyperion\nyx.py -m addOrder -o 123456 -l ngdc -p /var/www/vhosts/geoinsight.xyz/noaa.geoinsight.xyz/NOAA
```