
# Import the modules to be abed in the script

import psycopg2                                                 # import the 'psycopg2' module
from psycopg2 import sql                                        # import the 'sql' functionality from 'psycopg2'
import datetime                                                     # import the 'time' module
import os                                                       # import the 'os' module
import re                                                       # import the 're' module
import logging                                                  # import the 'logging' module
from psycopg2.extras import LoggingConnection
import sys
import subprocess
import time
import xlrd
import openpyxl
import shutil

# Report Month & Year variables

todays_date = datetime.datetime.now()
month = '%02d' % todays_date.month # abe this to add 0 the month number
year = str(todays_date.year)
start_time = time.time()
reporting_month = ('{0}_{1}'.format(year,month))
current_date = '{0}_{1}'.format(year, month)

if month == '01':
    previous_month = '12'
    previous_month_year = str(int(year)-1)
else:
    previous_month = (str(int(month)-1)).zfill(2)
    previous_month_year = year

print (month)
print (previous_month_year)
print (previous_month)
# Report location
ab_folder = '\\folder_path\\\\my_project_lmc\\Reports'

# Folder locations
project_folder = '\\folder_path\\\\my_project_lmc'
report_folder = '{0}\\Reports\\{1}\\{2}'.format(project_folder, year, month)
map_data_folder = '{0}\\Maps\\Data'.format(project_folder)
map_image_folder = '{0}\\Maps\\Images\\{1}\\{2}'.format(project_folder, year, month)
lmc_folder = '\\folder_path\\Maps\\Data'

# Create log file configuration
logging.basicConfig( filename ='\\folder_path\\\\my_project_lmc\\log.log', filemode='w',
                     format='%(asctime)s - %(message)s', level= logging.INFO)
#
# # create a results folder for the current year if it doesn't already exist
if not os.path.isdir('{0}'.format(report_folder)):
    os.makedirs('{0}'.format(report_folder))

# if the results spreadsheet already exists
if os.path.exists('{0}\\my_project_lmc_{1}_{2}.xlsx'.format(report_folder, year, month)):
    # continue with the script
    pass

# if the results spreadsheet does not already exist
else:

    # create a copy of the template excel spreadsheet in the results folder and rename it
    shutil.copy('{0}\\my_project_lmc_template.xlsx'
                .format(ab_folder),
                '{0}'.format(report_folder))
    os.rename('{0}\\my_project_lmc_template.xlsx'
              .format(report_folder),
              '{0}\\my_project_lmc_{1}_{2}.xlsx'
              .format(report_folder, year, month))

current_report = '{0}\\my_project_lmc_{1}_{2}.xlsx' \
    .format(report_folder, year, month)
# log start time
logging.info("script starts")

# Unocking sasines datasets
ab_datasets = ['a','b', 'c', 'd',
               'e']
ab_datasets_all = ['a','b','c', 'd', 'e',
               'f']

# Schemas
ab_schema = 'my_project'
bc_schema = 'land'

# Land datasets
lmc_ext_rg = 'boundary_{0}_{1}_extents_rg'.format(year, month)
lmc_ext_df = 'boundary_{0}_{1}_extents_df'.format(year, month)
lmc_report = 'boundary_{0}_report'.format(current_date)
ab_ss = 'ab_sheet_{0}_{1}'.format(year, month)
ab_ss_read_only = 'ab_sheet'

# LMC gpkg status
lmc_rg = 'lmc_rg'
lmc_df = 'lmc_df'

# define the operating system abername:
os_aber = os.getenv('abername').lower()

# define the abc database variables:
abc_db_name = 'my_db'
abc_db_host = 'xxxx.local'
abc_db_port = 1111
abc_db_aber = 'username'
abc_os_aber = os.getenv('abername').lower()

# extract the aber abc database password
db_password = None
location = "myfolder\\connectiondetails.txt".format(abc_os_aber)
with open(location,'r') as f:
    lines = f.readlines()
    for line in lines:
        if (re.split(r':', line)[0]) == abc_db_host:
            if re.split(r':',line)[3][0:5] == abc_db_aber[0:5]:
                abc_db_password = re.split(r':',line)[4].strip('\n')


# credentials for prod databse
prod_db_name = 'my_other_db'
prod_db_host = 'yyyy.local'
prod_db_port = 1111
prod_db_aber = 'username'
prod_os_aber = os.getenv('abername').lower()

# extract the user prod database password
prod_db_password = None
location = "myfolder\\connectiondetails.txt".format(prod_os_aber)
with open(location,'r') as f:
    lines = f.readlines()
    for line in lines:
        if (re.split(r':', line)[0]) == prod_db_host:
            if re.split(r':',line)[3][0:5] == prod_db_aber[0:5]:
                prod_db_password = re.split(r':',line)[4].strip('\n')

########################################################################################################################


def dbconnection(name, host, port, aber, password):
    ''' Fucntion to connected to the different
        databases in the script calling individual
         connection params '''

    conn = psycopg2.connect(connection_factory=LoggingConnection, dbname=name, host=host,
                            port=port, aber=aber,
                            password=password, sslmode='require')

    conn.autocommit = True

    # allow logging on queries
    conn.initialize(logger)

    cur = conn.cursor()
    return cur, conn


Postabcconnection = None
#
try:

    # create a logging object
    logger = logging.getLogger(__name__)

    # call connection function

    cur, conn = dbconnection(abc_db_name, abc_db_host, abc_db_port, abc_db_aber, abc_db_password)

    # lmc removal function
    # slithers under 50m2 are not included

    def lmc_polys(tablename, sourcetable, index):

        ''' Fucntion to identify intersecting
        layer polygons with RG and DF polygons
         Args: tablename - table to be created,
                sourcetable - lmc table
                 index - index to be created'''

        int_geoms = sql.SQL('''
                    DROP TABLE IF EXISTS {1}.{0};
                    CREATE TABLE {1}.{0}
                    AS
                    SELECT a.id, ST_collectionextract(ST_Intersection(a.geom, b.geom),3) as geom
                        FROM  {1}.{2} a, {3}.{4} b
                        WHERE ST_intersects(a.geom,b.geom)
                        AND ST_Area(ST_collectionextract(ST_Intersection(a.geom, b.geom),3)) > 50;
                        ''').format(sql.Identifier(tablename),
                                    sql.Identifier(ab_schema),
                                    sql.Identifier('{0}_new'.format(layer)),
                                    sql.Identifier(bc_schema),
                                    sql.Identifier(sourcetable)
                                    )

        cur.execute(int_geoms)

        add_geom_idx = sql.SQL('''
                                CREATE INDEX {0}
                                ON {1}.{2}
                                abING abcT(geom);
                    ''').format(sql.Identifier(index),
                                sql.Identifier(ab_schema),
                                sql.Identifier(tablename)
                                )

        cur.execute(add_geom_idx)

        logging.info("{0} table of  geoms created".format(layer))

    def update_date(sourcetable):
        '''
         Function that updates the
        month column when poly ints
        with df or rg table
        '''
        extent_updated = sql.SQL('''
                                WITH x
                                AS
                                (
                                SELECT DISTINCT (id) as id
                                FROM {0}.{1}
                                )
                                UPDATE {0}.{2} a
                                 SET extent_last_updated = %(today)s
                                 FROM x
                                 WHERE a.id = x.id''').format(sql.Identifier(ab_schema),
                                                              sql.Identifier(sourcetable),
                                                              sql.Identifier('{0}_new'.format(layer))
                                                              )

        cur.execute(extent_updated, {'today': todays_date})

    for layer in ab_datasets:
        # Table names

        lmc_rg_polys = '{0}_lmc_rg_polys'.format(layer)
        lmc_df_polys = '{0}_lmc_df_polys'.format(layer)

        # Create copy of previous month and new working table
        try:
            drop_table = sql.SQL(''' DROP TABLE IF EXISTS {0}.{1}
                            ''').format(sql.Identifier(ab_schema),
                                        sql.Identifier('{0}_new'.format(layer))
                                        )

            cur.execute(drop_table)

            create_copy_tables = sql.SQL('''
                            CREATE TABLE {1}.{0}
                            AS SELECT * FROM {1}.{3};

                            COMMENT ON TABLE {1}.{0} IS 'table created {4} {5} and is a copy of previous month working table';

                            CREATE TABLE {1}.{2}
                            AS SELECT * FROM {1}.{3};

                            COMMENT ON TABLE {1}.{2} IS 'table created {4} {5} and has lmc removed.
                            This table  will be replicated in ab_prod db';

                            ''').format(sql.Identifier('{0}_{1}_{2}'.format(layer, year, month)),
                                        sql.Identifier(ab_schema),
                                        sql.Identifier('{0}_new'.format(layer)),
                                        sql.Identifier(layer),
                                        sql.Identifier(month),
                                        sql.Identifier(year)
                                        )

            cur.execute(create_copy_tables)

            logging.info("{0} new tables copied".format(layer))
            print("{0} dropped".format(layer))

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

        # Alter working table
        try:
            rename_columns = sql.SQL('''
                                        DO $$
                                        BEGIN
                                        IF EXISTS(SELECT *
                                                 FROM  information_schema.columns
                                                 WHERE table_name = %(table)s AND column_name='oid')
                                                  THEN
                                                  ALTER TABLE {0}.{1} rename \
                                                  COLUMN "oid" TO "tempname";
                                                  END IF;
                                                  END $$''').format(sql.Identifier(ab_schema),
                                                                    sql.Identifier('{0}_new'.format(layer))
                                                                    )
            cur.execute(rename_columns, {'table': '{0}_new'.format(layer)})

            add_index = sql.SQL('''
                            CREATE INDEX {0}
                            ON {1}.{2} abING abcT(geom);
                            ''').format(sql.Identifier('{0}{1}_geom'.format(layer, reporting_month)),
                                        sql.Identifier(ab_schema),
                                        sql.Identifier('{0}_new'.format(layer))
                                        )
            cur.execute(add_index)

            logging.info("{0} spatial index index added to working table".format(layer))

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

        # Try to make all geoms valid

        try:

            update_geom = sql.SQL('''
                            UPDATE {1}.{0} a
                            SET geom =  ST_Buffer(geom, 0)
                            WHERE NOT ST_IsValid(geom);
                            ''').format(sql.Identifier('{0}_new'.format(layer)),
                                        sql.Identifier(ab_schema)
                                        )

            cur.execute(update_geom)

            logging.info("{0} geom converted to valid".format(layer))

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)

        # Create table of int RG & DF geoms. 0.005 abed to eradicate slithers

        try:

            lmc_polys(lmc_rg_polys, lmc_ext_rg, '{0}_{1}_lmc_polys_rg_geom'.format(layer, month))
            lmc_polys(lmc_df_polys, lmc_ext_df, '{0}_{1}_lmc_polys_df_geom'.format(layer, month))

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

        # Add update date

        try:

            update_date(lmc_rg_polys)
            update_date(lmc_df_polys)

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

        # Combine the RG and DF polygons
        try:
            combine_rg_df = sql.SQL('''DROP TABLE IF EXISTS {1}.{0};
                        CREATE TABLE {1}.{0}
                        AS
                        WITH x AS
                        (
                        SELECT  geom
                        FROM
                                (
                                    SELECT geom FROM {1}.{2}
                                    UNION ALL
                                    SELECT geom FROM {1}.{3}
                                ) x

                        GROUP BY  geom
                        )
                        select st_union(geom) as geom
                        FROM x;
                                 ''').format(sql.Identifier('{0}_lmc_polys'.format(layer)),
                                             sql.Identifier(ab_schema),
                                             sql.Identifier('{0}_lmc_rg_polys'.format(layer)),
                                             sql.Identifier('{0}_lmc_df_polys'.format(layer))
                                             )

            cur.execute(combine_rg_df)

            logging.info("{0} table of combined geoms created".format(layer))

            # add index and delete line intersects < 1 ###put this in top rpid
            add_index = sql.SQL('''
                            CREATE INDEX {0}
                            ON {1}.{2} abING abcT(geom);

                            DELETE FROM {1}.{2}
                            WHERE ST_area(geom)<1
                            ''').format(sql.Identifier('{0}_{1}_lmc_polys_geom'.format(layer, reporting_month)),
                                        sql.Identifier(ab_schema),
                                        sql.Identifier('{0}_lmc_polys'.format(layer))
                                        )
            cur.execute(add_index)

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

        # update geometry of polygons to that of geometry not int RG/DF land
        # 'update' column populated to this month where change has been made
        # create table of int geoms from _new where they int with LMC and also return geoms which dont int with any LMC
        try:
            geom_difference = sql.SQL('''
                                        DROP TABLE IF EXISTS {0}.{1};
                                        CREATE TABLE {0}.{1}
                                        AS
                                        SELECT a.id, COALESCE(ST_Difference(geom,
                                                                (SELECT ST_Union(b.geom)
                                                                 FROM {0}.{2} a, {0}.{3} b
                                                                 WHERE ST_Intersects(a.geom, b.geom)
                                                                 )), a.geom) as geom
                                        FROM {0}.{2} a;
                                ''').format(sql.Identifier(ab_schema),
                                            sql.Identifier('{0}_temp'.format(layer)),
                                            sql.Identifier('{0}_new'.format(layer)),
                                            sql.Identifier('{0}_lmc_polys'.format(layer))
                                            )

            cur.execute(geom_difference)

            logging.info("{0} table geom update with difference".format(layer))

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

        # delete the polygons in main table which have an id in the diff table but the geometry in it is 0.
        # these have been returned as the polygon has completely been covered by a df or rg
        # and the id is still there due to COALESCE returning the original geom

        try:
            delete_geoms = sql.SQL('''
                                    DELETE FROM {0}.{1}
                                    WHERE id IN (SELECT id
                                    FROM {0}.{2}
                                    WHERE ST_area(geom) = 0);
                                    ''').format(sql.Identifier(ab_schema),
                                                sql.Identifier('{0}_new'.format(layer)),
                                                sql.Identifier('{0}_temp'.format(layer))
                                                )
            cur.execute(delete_geoms)

            logging.info("{0} rg/df polygons deleted from table".format(layer))
        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

        # convert all multi to single poly (to match main table geom type)

        try:
            multi_to_single_polys = sql.SQL('''
                                    DROP TABLE IF EXISTS {0}.{1};
                                    CREATE TABLE {0}.{1}
                                    AS SELECT id, ST_GeometryN(geom, generate_series(1, ST_NumGeometries(geom))) AS geom
                                    FROM {0}.{2}
                                    WHERE ST_area(geom) > 0 order by id ;
                                          ''').format(sql.Identifier(ab_schema),
                                                      sql.Identifier('{0}_temp_singles'.format(layer)),
                                                      sql.Identifier('{0}_temp'.format(layer))
                                                      )
            cur.execute(multi_to_single_polys)

            logging.info("{0} temp muli polygons converted to single ".format(layer))
        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

        # create table of all attributes of the int geoms and the new int geom  to replace the old

        try:
            single_poly_attributes = sql.SQL( '''
                                                DROP TABLE IF EXISTS {0}.{1};
                                                CREATE TABLE {0}.{1}
                                                AS SELECT a.*, b.geom as newgeom
                                                FROM {0}.{2} a, {0}.{3} b
                                                WHERE a.id = b.id
                                                AND st_geometrytype(b.geom) = 'ST_Polygon';
                                                ''').format(sql.Identifier(ab_schema),
                                                            sql.Identifier('{0}_temp_single_attributes'.format(layer)),
                                                            sql.Identifier('{0}_new'.format(layer)),
                                                            sql.Identifier('{0}_temp_singles'.format(layer))
                                                            )

            cur.execute(single_poly_attributes)

            logging.info("{0} attributes assigned to temp table ".format(layer))

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

        # update to original geom field

        try:
            update_orig_geom = sql.SQL('''
                                        UPDATE {0}.{1}
                                        SET geom = newgeom;

                                        ALTER TABLE {0}.{1}
                                        drop column newgeom;
                                        ''').format(sql.Identifier(ab_schema),
                                                    sql.Identifier('{0}_temp_single_attributes'.format(layer))
                                                    )
            cur.execute(update_orig_geom)

            logging.info("{0} updated original geom from newgeom ".format(layer))
        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

        #  delete all the old polys in the table

        try:
            delete_orig_geom = sql.SQL('''
                                        DELETE  FROM {0}.{1}
                                        WHERE id IN (SELECT DISTINCT (id)
                                        FROM {0}.{2});

                                        INSERT INTO {0}.{1}
                                        (SELECT * FROM {0}.{2});
                                        ''').format(sql.Identifier(ab_schema),
                                                    sql.Identifier('{0}_new'.format(layer)),
                                                    sql.Identifier('{0}_temp_single_attributes'.format(layer))
                                                    )
            cur.execute(delete_orig_geom)

        except psycopg2.OperationalError as e:
            print('{0}').format(e)

            logger.error('{0}', e)
            sys.exit(1)

        # create new id to make unique
    for layer in ab_datasets_all:
        try:
            update_unique_id = sql.SQL('''
                                    ALTER TABLE {0}.{1}
                                    DROP COLUMN id;

                                    ALTER TABLE {0}.{1}
                                    ADD COLUMN id serial ;
                                    ''').format(sql.Identifier(ab_schema),
                                                sql.Identifier('{0}_new'.format(layer))
                                                )
            cur.execute(update_unique_id)

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

    # update area ha for each poly
    # 0.05 ha slithers removed
        try:
            update_hectarage = sql.SQL('''
                                    UPDATE {0}.{1}
                                    SET hectarage = ST_area(geom)*0.0001
                                    ''').format(sql.Identifier(ab_schema),
                                                sql.Identifier('{0}_new'.format(layer))
                                                )

            cur.execute(update_hectarage)

            delete_slithers = sql.SQL('''
                                    DELETE FROM {0}.{1} WHERE hectarage < 0.05
                                    ''').format(sql.Identifier(ab_schema),
                                                sql.Identifier('{0}_new'.format(layer))
                                                )

            cur.execute(delete_slithers)

            logging.info("{0} hectarge updated and slithers deleted".format(layer))

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

        # insert values of each dataset in to the table to be output
        try:
            insert_ab_values = sql.SQL( '''
                                INSERT INTO {0}.{1}
                                (dataset, current_hectarage)
                                 VALUES(%(dataset)s,
                                (SELECT ST_Area(ST_union(geom))*0.0001
                                 FROM {0}.{2}))
                                ''').format(sql.Identifier(ab_schema),
                                            sql.Identifier('ab_{0}_{1}_figures'.format(year, month)),
                                            sql.Identifier('{0}'.format(layer)),
                                            sql.Identifier('{0}_new'.format(layer))
                                            )
            cur.execute(insert_ab_values, {'dataset': layer})

            update_lmc_values = sql.SQL('''
                                UPDATE {0}.{1} a
                                SET current_lmc = (a.current_hectarage/8007824.75)*100;

                                UPDATE {0}.{1} a
                                SET monthly_change = (a.current_hectarage - b.current_hectarage),
                                current_lmc = (a.current_hectarage/8007824.75)*100,
                                monthly_change_lmc = a.current_lmc - b.current_lmc
                                FROM {0}.{2} b
                                WHERE a.dataset = b.dataset;

                                ''').format(sql.Identifier(ab_schema),
                                            sql.Identifier('ab_{0}_{1}_figures'.format(year, month)),
                                            sql.Identifier('ab_{0}_{1}_figures'.format(previous_month_year, previous_month)))

            cur.execute(update_lmc_values)

            insert_ss_values = sql.SQL('''
                                            WITH x AS
                                                    (
                                                    SELECT ST_Area(ST_union(geom))*0.0001 as ss_area
                                                    FROM {0}.{1}
                                                    WHERE  ab_confidence_level IN (1, 2, 3)
                                                    )
                                                    UPDATE {0}.{2} a
                                                    SET sheet_land =  ss_area
                                                    FROM x
                                                    WHERE a.dataset = %(dataset)s
                                            ''').format(sql.Identifier(ab_schema),
                                                        sql.Identifier('{0}_new'.format(layer)),
                                                        sql.Identifier('ab_{0}_{1}_figures'.format(year, month))
                                                        )

            cur.execute(insert_ss_values, {'dataset': layer})

            update_ss_lmc_values = sql.SQL('''
                                            UPDATE {0}.{1} a
                                            SET sheet_lmc = (a.sheet_land/8007824.75)*100;
                                            ''').format(sql.Identifier(ab_schema),
                                                        sql.Identifier('ab_{0}_{1}_figures'.format(year, month)),
                                                       )
            cur.execute(update_ss_lmc_values)

            logging.info("{0} figures added to figures table ".format(layer))
        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

        # Output monthly figures to monthly spradsheet

        try:
            extract_monthly_figures = sql.SQL('''SELECT dataset,
                                                        current_hectarage,
                                                        monthly_change,
                                                        current_lmc,
                                                        monthly_change_lmc,
                                                        sheet_land,
                                                        sheet_lmc
                                                FROM {0}.{1};
                                                ''').format(sql.Identifier(ab_schema),
                                                            sql.Identifier('ab_{0}_{1}_figures'.format(year, month))
                                                            )
            cur.execute(extract_monthly_figures)
            results = cur.fetchall()

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

        # Output data to monthly report
        try:
            # open the current report
            tab = 'ab_lmc'
            current_wb = openpyxl.load_workbook(filename=current_report)
            current_sheet = current_wb['{0}'.format(tab)]

            # loop through each row of results
            for i in range(len(results)):

                # loop through each column in the row
                for j in range(len(results[i])):
                    # assign the values to the lmc report spreadsheet
                    current_sheet.cell(row=i + 3, column=j + 1, value=results[i][j])

            # save the report spreadsheet
            current_wb.save(current_report)

            # add event to the log file
            logger.info('{:>4} mins {:>2} secs\t{}: Report exported'.format(
                int((time.time() - start_time) / 60),
                int((time.time() - start_time) % 60),
                tab[0]))

            # if the attempt fails
        except xlrd.XLRDError as e:

                print("Report not exported as cannot export to Excel file")

            # print statement when report exported
        print("Report exported")

        # drop tables

        try:
            drops_tables = sql.SQL('''
                                    DROP TABLE IF EXISTS {0}.{1};
                                    DROP TABLE IF EXISTS {0}.{2};
                                    DROP TABLE IF EXISTS {0}.{3};
                                    DROP TABLE IF EXISTS {0}.{4};
                                    DROP TABLE IF EXISTS {0}.{5};
                                    DROP TABLE IF EXISTS {0}.{6};
                                    ''').format(sql.Identifier(ab_schema),
                                                sql.Identifier('{0}_temp_single_attributes'.format(layer)),
                                                sql.Identifier('{0}_temp_singles'.format(layer)),
                                                sql.Identifier('{0}_lmc_polys'.format(layer)),
                                                sql.Identifier('{0}_lmc_df_polys'.format(layer)),
                                                sql.Identifier('{0}_lmc_rg_polys'.format(layer)),
                                                sql.Identifier('{0}_temp'.format(layer))
                                                )
            cur.execute(drops_tables)

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)


except psycopg2.DatabaseError as e:
    logging.exception(e)
    print('Error %s' % e)
    sys.exit(1)

#OPEN 2nd POSTabc CONNECTION


def columns_triggers(trigger_name, calling_function):
    create_triggers = sql.SQL('''   DROP TRIGGER IF EXISTS {2} ON {0}.{1};

                                    CREATE TRIGGER {2}
                                    BEFORE INSERT OR UPDATE
                                    ON {0}.{1}
                                    FOR EACH ROW
                                    EXECUTE FUNCTION {0}.{3}();
                                ''').format(sql.Identifier(ab_schema),
                                            sql.Identifier(layer),
                                            sql.Identifier(trigger_name),
                                            sql.Identifier(calling_function)
                                            )

    logging.info("{0} trigger added".format(trigger_name))

    cur.execute(create_triggers)
Postabcconnection = None
try:

    # create a logging object
    logger = logging.getLogger(__name__)

    # call connection function
    cur, conn = dbconnection(prod_db_name, prod_db_host, prod_db_port, prod_db_aber, prod_db_password)

    for layer in ab_datasets_all:
# Create Triggers on tables which cll existing fucntions

        try:
            columns_triggers('class_to_uppercase', 'uppercase_class')
            columns_triggers('holder_to_uppercase', 'uppercase_holder')
            columns_triggers('searcher_to_uppercase', 'uppercase_searcher')
            columns_triggers('ss_number_to_uppercase', 'uppercase_ss_number')

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

# Create Triggers on tables which cll existing fucntions


        # create copy of existing table to update with LMC extents. Also fixes copy table bug
        try:
            drop_ab_table = sql.SQL('''DROP TABLE IF EXISTS {0}.{1} CASCADE;;'''
                                    ).format(sql.Identifier(ab_schema),
                                             sql.Identifier('{0}'.format(layer))
                                             )

            cur.execute(drop_ab_table)
        except psycopg2.OperationalError as e:
                    print('{0}').format(e)
                    logger.error('{0}', e)
                    sys.exit(1)

        try:
            # copy table from abc to dw prod
            copy_data = '''ogr2ogr -f PostgreSQL -overwrite\
                    PG:"dbname={5} host={6} port={7} aber={8} password={9} sslmode='require'"
                     -lco GEOMETRY_NAME=geom PG:"dbname={0} host={1} port={2} aber={3} password={4} sslmode='require'" -sql\
                    "SELECT * FROM my_project.{10}_new;" -nln "my_project.{10}"'''\
                    .format(abc_db_name, abc_db_host, abc_db_port, abc_db_aber, abc_db_password,
                            prod_db_name, prod_db_host, prod_db_port, prod_db_aber, prod_db_password, layer)

            subprocess.call(copy_data)

            logging.info("{0} table re-imported to DW".format(layer))

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

        try:
            alter_output = sql.SQL(''' ALTER TABLE {0}.{1}
                                        DROP COLUMN IF EXISTS ogc_fid;

                                        DO $$
                                                    BEGIN
                                                    IF EXISTS(SELECT *
                                                              FROM  information_schema.columns
                                                              WHERE table_name =%(table)s  AND column_name='tempname')
                                                              THEN
                                                              ALTER TABLE {0}.{1} rename
                                                              COLUMN "tempname" TO "oid";
                                                    END IF;
                                        END $$;
                                             ''').format(sql.Identifier(ab_schema),
                                                         sql.Identifier('{0}'.format(layer))
                                                         )
            cur.execute(alter_output, {'table': '{0}'.format(layer)})

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

        # delete all main table table to be update new data to it

        try:
            replace_table = sql.SQL('''
                                    ALTER TABLE {0}.{1}
                                    ADD CONSTRAINT {4} PRIMARY KEY (id);

                                    CREATE INDEX {2}
                                    ON {0}.{1} abING abct
                                    (geom);

                                    CREATE INDEX {3}
                                    ON {0}.{1} abING btree
                                    (id ASC NULLS LAST);

                                    COMMENT ON TABLE {0}.{1} IS 'table created {5} {6} with previous months LMC removed';
                                    ''').format(sql.Identifier(ab_schema),
                                                sql.Identifier('{0}'.format(layer)),
                                                sql.Identifier('{0}_{1}_geom_geom'.format(layer, reporting_month)),
                                                sql.Identifier('{0}_{1}_id_idx'.format(layer, reporting_month)),
                                                sql.Identifier('{0}_pk'.format(layer, reporting_month)),
                                                sql.Identifier(month),
                                                sql.Identifier(year)
                                                )

            cur.execute(replace_table)
            logging.info("{0} old data replaced and new_update table dropped".format(layer))

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

        # Change table owner to ab_rw so ab team can access and edit
        try:
            alter_owner = sql.SQL('''
                                    ALTER TABLE {0}.{1}
                                    OWNER to my_project_rw;
                                    ''').format(sql.Identifier(ab_schema),
                                                sql.Identifier(layer))
            cur.execute(alter_owner)

            logging.info("{0} owner changed to ab_rw".format(layer))

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

except psycopg2.DatabaseError as e:
    logging.exception(e)
    print('Error %s' % e)
    sys.exit(1)
# disconnect from Postabc
if Postabcconnection:
    Postabcconnection.close()

print("Part 1 complete. LMC extents removed and datsets created")

######################################################################################################################
######################################################################################################################
#
# PART 2 - Map Creation

#Functions

def makefolders(folder_name):
    ''' function to create folders
     if they do not exist '''
    if not os.path.isdir('{0}\\{1}\\{2}'.format(folder_name, year, month)):
        os.makedirs('{0}\\{1}\\{2}'.format(folder_name, year, month))

        logging.info("{0} created".format(folder_name))

def remove_old_data(dataset):
    ''' function to reomve old
     data from folders '''
    if os.path.exists('{0}\\{1}.gpkg'.format(map_data_folder, dataset)):
        os.remove('{0}\\{1}.gpkg'.format(map_data_folder, dataset))

    logging.info("{0} removed".format(dataset))
#
#
def update_ss_table(ss_sorcetable):
    ''' Function that inserts all from each
     dataset in to a search sheet monthly table'''
    ss_extent_updated = sql.SQL('''
                                    INSERT INTO {0}.{1}
                                    (
                                    original_id, county, indicative_title_holders,
                                    potential_sheet_numbers,subjects_of_search,
                                    ab_confidence_level, geom
                                    )
                                    (
                                    SELECT id, county, indicative_title_holders,
                                     potential_sheet_numbers,subjects_of_search,
                                     ab_confidence_level, geom
                                            FROM {0}.{2}
                                                WHERE ab_confidence_level IN (1, 2, 3));
                                 ''').format(sql.Identifier(ab_schema),
                                             sql.Identifier(ab_ss),
                                             sql.Identifier(ss_sorcetable)
                                             )

    logging.info("{0} geoms added to ab_ss".format(ab_datasets))

    cur.execute(ss_extent_updated)


def export_monthly_data(lmc_ab_layer, schema,output_name,folder_name):
    ''' function that selects features from table
        and writes them to the output folder'''
    sql_current_rg_extents = "SELECT * FROM {0}.{1} WHERE geom IS NOT NULL;" \
        .format(schema, lmc_ab_layer)

    # Export the new monthly search sheet table extents as a geopackage
    export_extents_table = \
        "ogr2ogr \
         -f \"GPKG\" \
         {0}\\{3}.gpkg \
         PG:\"{1} \" \
         -nln {3} \
         -sql \"{2}\"".format(folder_name, database_credentials, sql_current_rg_extents, output_name)
    subprocess.call(export_extents_table)

    logging.info("{0} copied to folder".format(output_name))


#Database connection details

exec(open('\\folder_path\\\\database_connections\\database_connections.py').read())

# OPEN POSTabc CONNECTION
Postabcconnection = None
try:

    # create a logging object
    logger = logging.getLogger(__name__)

    conn = psycopg2.connect(connection_factory=LoggingConnection, dbname=abc_db_name, host=abc_db_host,
                            port=abc_db_port, aber=abc_db_aber,
                            password=abc_db_password, sslmode='require')

    conn.autocommit = True

    # allow logging on queries
    conn.initialize(logger)

    cur = conn.cursor()

    #  create new monthly ab Search Sheet table for read only teams
    try:
        create_ss_table = sql.SQL('''
                                  DROP TABLE IF EXISTS {0}.{1};

                                  CREATE TABLE {0}.{1}
                                  (
                                    original_id integer,
                                    county character varying,
                                    indicative_title_holders character varying,
                                    potential_sheet_numbers character varying,
                                    subjects_of_search character varying,
                                    ab_date_completed date,
                                    ab_confidence_level integer,
                                    geom GEOMETRY (POLYGON,27700))
                                    ;

                                  CREATE INDEX {2} ON {0}.{1} abING abcT(geom);

                                  ALTER TABLE {0}.{1}
                                  ADD COLUMN id serial
                                   ''').format(sql.Identifier(ab_schema),
                                               sql.Identifier(ab_ss),
                                               sql.Identifier('{0}_geom_idx'.format(ab_ss)))

        cur.execute(create_ss_table)
        logging.info("{0} created".format(ab_ss))

    except psycopg2.OperationalError as e:
        print('{0}').format(e)
        logger.error('{0}', e)
        sys.exit(1)

#     #####################################################################################
#     ######################################################################################
#     # COMMENTED OUT UNTIL this layer has to be provided monthly. One off cut has been provided
#     #########################################################################################
#     # # drop and recreate new ab_ss_read_only table in the DW
#     # try:
#     #     drop_ab_table = sql.SQL('''DROP TABLE IF EXISTS {0}.{1} CASCADE;;'''
#     #                             ).format(sql.Identifier(ab_schema),
#     #                                      sql.Identifier('{0}'.format(ab_ss_read_only))
#     #                                      )
#     #
#     #     cur.execute(drop_ab_table)
#     # except psycopg2.OperationalError as e:
#     #     print('{0}').format(e)
#     #     logger.error('{0}', e)
#     #     sys.exit(1)
#     #
#     # try:
#     #     # copy ab_ss table from abc to DW prod
#     #     copy_data = '''ogr2ogr -f PostgreSQL -overwrite\
#     #             PG:"dbname={5} host={6} port={7} aber={8} password={9} sslmode='require'"
#     #              -lco GEOMETRY_NAME=geom PG:"dbname={0} host={1} port={2} aber={3} password={4} sslmode='require'" -sql\
#     #             "SELECT * FROM my_project.{10};" -nln "my_project.{11}"'''\
#     #             .format(abc_db_name, abc_db_host, abc_db_port, abc_db_aber, abc_db_password,
#     #                     prod_db_name, prod_db_host, prod_db_port, prod_db_aber, prod_db_password,
#     #                     ab_ss, ab_ss_read_only)
#     #
#     #     subprocess.call(copy_data)
#     #
#     #     logging.info("{0} table imported to DW".format(ab_ss_read_only))
#     #
#     # except psycopg2.OperationalError as e:
#     #     print('{0}').format(e)
#     #     logger.error('{0}', e)
#     #     sys.exit(1)
# #######################################################################################################
#
    # loop through all ab datasets to be added to the main table
    for layer in ab_datasets_all:
        try:
            update_ss_table(layer)

        except psycopg2.OperationalError as e:
            print('{0}').format(e)
            logger.error('{0}', e)
            sys.exit(1)

    # create folders for the current year if they don't already exist
    try:
        makefolders(map_data_folder)
        makefolders(map_image_folder)

    except psycopg2.OperationalError as e:
        print('{0}').format(e)
        logger.error('{0}', e)
        sys.exit(1)

        # define the database connection details
    database_credentials = "host='{0}' port='{1}' aber='{2}' dbname='{3}' password='{4}' sslmode='require'" \
        .format(abc_db_host, abc_db_port, abc_db_aber, abc_db_name, abc_db_password)

    # delete previous datasets from map folder
    try:
        remove_old_data('lmc_ext_rg')
        remove_old_data('lmc_ext_df')
        remove_old_data('ab_sheet')

    except psycopg2.OperationalError as e:
        print('{0}').format(e)
        logger.error('{0}', e)
        sys.exit(1)

    # export the layers and parameaters to the mpa project
    try:
        export_monthly_data(ab_ss, ab_schema, 'ab_sheet', map_data_folder)

        if not os.path.isdir('{0}\\{1}_current.gpkg'.format(lmc_folder, lmc_rg)):
            export_monthly_data(lmc_ext_rg, bc_schema, 'lmc_ext_rg', lmc_folder)

        if not os.path.isdir('{0}\\{1}_current.gpkg'.format(lmc_folder, lmc_df)):
            export_monthly_data(lmc_ext_df, bc_schema, 'lmc_ext_df', lmc_folder)

    except psycopg2.OperationalError as e:
        print('{0}').format(e)
        logger.error('{0}', e)
        sys.exit(1)

    try:
        # define the SQL to extract the dynamic text for the map layouts
        # this SQL calculates ab LMC %
        sql_layout_text = '''
                            WITH  x as (
                                        SELECT SUM(sheet_land) as area
                                        FROM {0}.{1}),
                                        y as (SELECT SUM(sheet_lmc)  as ab_percentage
                                        FROM {0}.{1}
                                        )
                                        SELECT CONCAT(TO_CHAR(EXTRACT(DAY FROM CURRENT_DATE),'fm00'),' ',
                                        LEFT(TO_CHAR(CURRENT_DATE,'Month'),3),' ',
                                        EXTRACT(YEAR FROM CURRENT_DATE)) AS report_date, '{5}' AS year_month,
                                        CAST(CAST(y.ab_percentage AS decimal(10,1)) AS text) as ab_percentage,
                                        CAST(CAST(a.percent_rg AS decimal(10,1)) AS text) AS percent_rg,
                                        CAST(CAST(a.percent_df AS decimal(10,1)) AS text) AS percent_df,
                                        '2022' AS this_year, '2021' AS last_year,
                                        ST_PointFromText('POINT(0 0)', 27700) AS geom
                                        FROM {2}.{6} a, y
                                            WHERE a.geog_name = 'Total'
                            '''.format(ab_schema, 'ab_{0}_{1}_figures'.format(year, month), bc_schema, month, year, current_date,
                                       lmc_report)

        # Export the layout text
        export_layout_text = \
            "ogr2ogr \
             -f \"GPKG\" \
             {0}\\layout_text.gpkg \
             PG:\"{1} \"\
             -nln layout_text \
             -sql \"{2}\"".format(map_data_folder, database_credentials, sql_layout_text)
        subprocess.call(export_layout_text)

        # print statement when geopackages exported
        print("Layout text exported")

    except psycopg2.OperationalError as e:
        print('{0}').format(e)
        logger.error('{0}', e)
        sys.exit(1)

    # try:
    #     map(update_ss_table(ab_datasets))
    #     #{'table': '{0}'.format(layer)})
    #
    # except psycopg2.OperationalError as e:
    #     print('{0}').format(e)
    #     logger.error('{0}', e)
    #     sys.exit(1)


except psycopg2.DatabaseError as e:
    logging.exception(e)
    print('Error %s' % e)
    sys.exit(1)
# disconnect from Postabc
if Postabcconnection:
    Postabcconnection.close()
