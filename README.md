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
# GUIDS
Add orders which are from NOAA to DATABASE_VIIRS 
```python D:\NyxHyperion\nyx.py -m addOrder -o 123456 -l ngdc -p /var/www/vhosts/geoinsight.xyz/noaa.geoinsight.xyz/NOAA
