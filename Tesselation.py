
# Tessellation code used to reduce the computational complexity of the automatic thresholding algorithm
def findThresholdSet (inRaster, blocksize = 5000):

    """    
    param inRaster:  Path where the image in tiff format is located (I expect a large image)
    param blocksize: size of the window that will explore or go through the large image the function  
    findThreshold will be executed on small blocks which have this size by default is 5000
    return: A dictionary called tDict that contains the min, max, average threshold values and the set of thresholds
    that were computed over the windows (for exploration purposes) , we will test with sanghetha which of those is more suitable
    """

    myRaster = arcpy.Raster(inRaster)
    thresholds = []
    tDict = {}
    c = 0
    t = 0
    for x in range(0, myRaster.width, blocksize):
        for y in range(0, myRaster.height, blocksize):

            # Lower left coordinate of block (in map units)
            mx = myRaster.extent.XMin + x * myRaster.meanCellWidth
            my = myRaster.extent.YMin + y * myRaster.meanCellHeight
            # Upper right coordinate of block (in cells)
            lx = min([x + blocksize, myRaster.width])
            ly = min([y + blocksize, myRaster.height])

            # Extract data block
            block = arcpy.RasterToNumPyArray(myRaster, arcpy.Point(mx, my), lx-x, ly-y)
            #print(block)
            c += 1
            #print(str(c)+". Block")

            #Somtimes the findThreshold function fails that is why I added the try - except structuture
            try :
                t = findThreshold(block)
                print("Succeeded in block number " + str(c))
            except :

                print("Failed in block number "+str(c))
            #Values balow -40 are excluded since they correspond to the black strips that surround the image.
            if (t!=0 and t>= -40):
                thresholds.append(t)

    meanT = sum(thresholds)/len(thresholds)
    minT = min(thresholds)
    maxT = max(thresholds)
    tDict["mean"] = meanT
    tDict["minT"] = minT
    tDict["maxT"] = maxT
    tDict["Set"] = thresholds

    print (str(c) + " Blocks were evaluated")
    return tDict