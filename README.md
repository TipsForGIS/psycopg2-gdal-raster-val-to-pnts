* The code assumes you have a proper raster file and a point table in Postgres, and both are in the same projection.

* The code uploads the raster file into Postgres using the raster2pgsql command.

* Then it will create a copy table of the point table with an additional column called rast_val.

* This new column is then updated with pixel values intersecting with the points.

** You can use the json file to input your connection parameters.
