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
sys.path.append('C:\\Users\\MrRobot\\.snap\\snap-python')
from snappy import jpy
from snappy import ProductIO
from snappy import GPF
from snappy import HashMap
import numpy as np
import math
import matplotlib.pyplot as plt
import matplotlib.colors as colors
from osgeo import gdal, ogr, osr


def plotBand(product, band, vmin, vmax, title = ""):
    # Display a specific band with the matplotlib library
    band = product.getBand(band)

    w = band.getRasterWidth()
    h = band.getRasterHeight()

    band_data = np.zeros(w * h, np.float32)
    band.readPixels(0, 0, w, h, band_data)

    band_data.shape = h, w

    width = 12
    height = 12
    plt.figure(figsize=(width, height))
    plt.title(title)
    imgplot = plt.imshow(band_data, cmap=plt.cm.binary, vmin=vmin, vmax=vmax)
    
    return imgplot 

def readMetadata (sentinel_1_path, toPrint = True):    
    # Extract information about the Sentinel-1 GRD product:
    sentinel_1_metadata = "manifest.safe"
    s1prd = "data/%s/%s.SAFE/%s" % (sentinel_1_path, sentinel_1_path, sentinel_1_metadata)
    reader = ProductIO.getProductReader("SENTINEL-1")
    product = reader.readProductNodes(s1prd, None)
    width = product.getSceneRasterWidth()
    height = product.getSceneRasterHeight()
    name = product.getName()
    band_names = product.getBandNames()
    
    if toPrint:
        print("Product: %s, %d x %d pixels" % (name, width, height))
        print("Bands:   %s" % (list(band_names)))
    return product

def subset (product, x, y, width, heigth, toPrint = True):    
    # subset of the Sentinel-1 GRD product by specify a rectangle whose top most left corner is defined by x and y coordinates
    HashMap = jpy.get_type('java.util.HashMap')    
    GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()    
    parameters = HashMap()
    parameters.put('copyMetadata', True)
    parameters.put('region', "%s,%s,%s,%s" % (x, y, width, height))
    subset = GPF.createProduct('Subset', parameters, product)
    if toPrint:
        print("Bands:   %s" % (list(subset.getBandNames())))
    return subset

def radiometricCalibration (subset):   
    parameters = HashMap()
    parameters.put('auxFile', 'Latest Auxiliary File')
    parameters.put('outputSigmaBand', True)
    parameters.put('selectedPolarisations', 'VV')
    calibrate = GPF.createProduct('Calibration', parameters, subset)
    list(calibrate.getBandNames())
    return calibrate

def speckleFiltering (calibrate, toPrint = True):
    
    parameters = HashMap()    
    parameters.put('filter', 'Lee')
    parameters.put('filterSizeX', 7)
    parameters.put('filterSizeY', 7)
    parameters.put('dampingFactor', 2)
    parameters.put('edgeThreshold', 5000.0)
    parameters.put('estimateENL', True)
    parameters.put('enl', 1.0)    
    speckle = GPF.createProduct('Speckle-Filter', parameters, calibrate)    
    band_names = speckle.getBandNames()
    if toPrint: 
        print(list(band_names))
    return speckle

def geometricCorrection (speckle, toPrint = True):
    parameters = HashMap()    
    parameters.put('demName', 'SRTM 3Sec')
    parameters.put('externalDEMNoDataValue', 0.0)
    parameters.put('demResamplingMethod', "BILINEAR_INTERPOLATION")
    parameters.put('imgResamplingMethod', "BILINEAR_INTERPOLATION")
    parameters.put('pixelSpacingInMeter', 10.0)
    parameters.put('pixelSpacingInDegree', 0.0)
    parameters.put('mapProjection', "WGS84(DD)")    
    terrain = GPF.createProduct('Terrain-Correction', parameters, speckle)    
    if toPrint:
        print("Bands:   %s" % (list(terrain.getBandNames())))    
    
    return terrain
    


# These arguments correpond to the rectangle that I want to subset from the original image
x = 18727
y = 15438
width = 6186
height = 3942

#Define the product to process:
sentinel_1_path = "S1A_IW_GRDH_1SSV_20150122T030723_20150122T030752_004278_005347_8809"

product = readMetadata (sentinel_1_path, toPrint = True)
subset =  subset (product, x, y, width, height, toPrint = True)
calibrate = radiometricCalibration (subset)
speckle = speckleFiltering (calibrate, toPrint = True)
terrain = geometricCorrection (speckle, toPrint = True)
ProductIO.writeProduct(terrain, 'Terrain.tif', "Geotiff")


plotBand(subset, 'Amplitude_VV', 0, 750, "Subset Radiometric Calibration")
plotBand(speckle, 'Sigma0_VV', 0, 0.3, "Speckle Filtering")
plotBand(terrain, 'Sigma0_VV', 0, 0.3, "Terrain Correction")
