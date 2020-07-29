import os
import json
import psycopg2 as pg2
from osgeo import gdal
import datetime


# load configs from json file
with open('./ingest_config.json') as json_file:    
    params = json.load(json_file)

raster_file = params['raster_file']
raster_table_name = params['db_params']['raster_table_name']
points_table_name = params['db_params']['points_table_name']
host = params['db_params']['host']
user_name = params['db_params']['user_name']
password = params['db_params']['password']
db_name = params['db_params']['db_name']
port = params['db_params']['port']
schema = params['db_params']['schema']

# create the db connection and cursor
conn = pg2.connect(database=db_name,user=user_name,password=password,host=host,port=port)
conn.set_session(autocommit=True)
cur = conn.cursor()
print('database connected successfully!')
today = ''

########################
# user defined functions
########################

# check if raster_table_name eixsts
def check_raster_table_existance():
    # check if the vectorized_table_new exists in the database or not
    cur.execute(("SELECT EXISTS ("+
                " SELECT 1"+
                " FROM pg_tables"+
                " WHERE schemaname = '"+schema+"'"+
                " AND tablename = '"+raster_table_name+"');"))
    
    # the EXISTS postgres function returns boolean
    # so cur.fetchone()[0] will be either True or False 
    if cur.fetchone()[0]:
        print(schema+'.'+raster_table_name+' already existed')
        want_to_continue = input('Woud you like to proceed and overwrite the table? (y/n)')
        if want_to_continue.lower() == 'y':
            pass
        elif want_to_continue.lower() == 'n':
            print('program terminated')
            exit()
        else:
            print('You did not enter y or n!')
            check_raster_table_existance()
    else:
        pass

# check if points_table_name eixsts
def check_points_table_existance():
    # check if the vectorized_table_new exists in the database or not
    cur.execute(("SELECT EXISTS ("+
                " SELECT 1"+
                " FROM pg_tables"+
                " WHERE schemaname = '"+schema+"'"+
                " AND tablename = '"+points_table_name+"');"))
    
    if cur.fetchone()[0]:
        pass
    else:
        print(schema+'.'+points_table_name+' does not exist in the '+db_name+' database under the host: '+host)
        print('please make sure you entered the correct points_table_name param in the ingest_config.json file')
        exit()

# create a postgis raster table from raster_file
def raster2pgsql():

    # generate the raster2pgsql command
    raster2pgsql_cmd = 'raster2pgsql -I -c -C -s 3857 -t 225x225 -F -M '+raster_file+' '+schema+'.'+raster_table_name+' | PGPASSWORD='+password+' psql --user='+user_name+' --dbname='+db_name+' --host='+host+' --port='+port
    # run the raster2pgsql in bash
    os.system(raster2pgsql_cmd)
    print(raster_table_name+' table created from '+raster_file)

# add a column called raster_val to the points table
def create_point_table_copy():

    # get today's date to use it as a postfix for the table copy name
    global today
    today = datetime.datetime.now().strftime("_%b_%d_%y")
    
    # create the table copy using the structure of the points table
    cur.execute(('CREATE TABLE '+schema+'.'+points_table_name+today+' AS ('+
                ' SELECT *'+
                ' FROM '+schema+'.'+points_table_name+
                ' LIMIT 0);'))

    # add an extra column named rast_val to store the raster pixel vals into the intersected points
    cur.execute('ALTER TABLE '+schema+'.'+points_table_name+today+' ADD COLUMN IF NOT EXISTS rast_val double precision;')
    
    # create an andex for the geom column
    cur.execute('CREATE INDEX '+points_table_name+today+'_geom_idx ON '+schema+'.'+points_table_name+today+' USING gist (geom);')
    
    # confirmation messages
    print(schema+'.'+points_table_name+today+' table created')
    print('raster_val column and geom index created on '+schema+'.'+points_table_name+today)

# update raster_val column using the raster table pixel values
def upload_point_table_copy_data():

    cur.execute(('INSERT INTO '+schema+'.'+points_table_name+today+' ('+
                ' SELECT p.*, ST_Value(r.rast,p.geom)'+
                ' FROM '+schema+'.'+points_table_name+' AS p, '+schema+'.'+raster_table_name+' AS r'+
                ' WHERE ST_Intersects(r.rast,p.geom)'+
                ');'
                ))

    cur.execute('VACUUM '+schema+'.'+points_table_name+today)

    print(schema+'.'+points_table_name+today+' table is uploaded with '+schema+'.'+points_table_name+' data, plus rast_val column holding raster values for each point')

####################
####### main #######
####################
if __name__ == '__main__':
    
    check_raster_table_existance()
    
    check_points_table_existance()

    raster2pgsql()

    create_point_table_copy()

    upload_point_table_copy_data()

    conn.close()