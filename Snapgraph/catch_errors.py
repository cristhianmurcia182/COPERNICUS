# -*- coding: utf-8 -*-
"""
Created on Thu Apr 27 11:09:09 2017

@author: MrRobot
"""

# Importing libraries
# Might be useful to set your working directory before hand in my case D:\COPERNICUS
import sys

# This path might change in your pc, just look at the path where your snap-python folder is located
# In some computers this line is not required
# sys.path.append('C:\\Users\\MrRobot\\.snap\\snap-python')
# from snappy import jpy

from snappy import ProductIO
from snappy import GPF
from snappy import HashMap
import os


def readMetadata(inputPath, name, toPrint=True):
    # Extract information about the Sentinel-1 GRD product:

    # s1prd = inputPath + "%s.SAFE/manifest.safe" % (name)
    s1prd = inputPath + "%s" % (name)
    reader = ProductIO.getProductReader("SENTINEL-1")
    product = reader.readProductNodes(s1prd, None)
    metadata = {}
    width = product.getSceneRasterWidth()
    height = product.getSceneRasterHeight()
    name = product.getName()
    band_names = list(product.getBandNames())

    params = HashMap()
    orbit = GPF.createProduct("Apply-Orbit-File", params, product)
    orbit = orbit.getName()

    metadata["name"] = name
    metadata["band_names"] = band_names
    metadata["width"] = width
    metadata["heigth"] = height
    metadata["orbit"] = orbit

    if toPrint:
        print("Product: %s, %d x %d pixels" % (name, width, height))
        print("Bands:   %s" % (band_names))
    return metadata


def checkMissingFiles(inputPath, name):
    metadata = readMetadata(inputPath, name, toPrint=False)
    o = metadata["orbit"]
    if 'Amplitude_VH' not in metadata["band_names"]:
        return False
    elif o[-3:] != 'Orb':
        return False
    else:
        return True

# inputPath = "D:\COPERNICUS\\12_Preprocessing\input\\"
# name = 'S1A_IW_GRDH_1SDV_20170508T054129_20170508T054154_016486_01B519_019A.zip'
#
# checkMissingFiles(inputPath, name)
#
# names = os.listdir(inputPath)
# for name in names:
#     readMetadata(inputPath, name, toPrint=True)
