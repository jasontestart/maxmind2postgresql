import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
from zipfile import ZipFile
import psycopg2
import csv
import tempfile

DB_NAME = "maxmind"
mylocale = 'en'

# Helper function
# Load configuration data from the GeoIP.conf file generated
# by Maxmind for their geoipupdate program.
def get_geoip_config():

    result = { 'AccountID' : None, 'LicenseKey' : None, 'EditionIDs' : None }

    conffilename = 'GeoIP.conf'
    lines = []
    with open(conffilename,'r') as f:
        lines = f.readlines()

    for line in lines:
        if line.isspace():
            continue
        if line[0] == '#':
            continue
        key,value = line.rstrip().split(' ', 1)
        if key in result.keys():
            if key == 'EditionIDs':
                result[key] = value.split(' ')
            else:
                result[key] = value
    return result

# Helper function
# Return the date of the latest version of Maxmind data stored in the database.
def get_latest_import_date(edition):
    result = None

    if edition == 'GeoLite2-ASN':
        query = 'SELECT MAX(last_modified) FROM geoip2_asn;'
    else:
        query = 'SELECT MAX(last_modified) FROM geoip2_location UNION SELECT MAX(last_modified) FROM geoip2_network;'

    conn = psycopg2.connect(database=DB_NAME)
    cur = conn.cursor()
    cur.execute(query)
    return_row = cur.fetchone()
    if return_row:
        result = return_row[0]
    conn.close()
    return result

# Helper function
# Fetch CSV data files from maxmind if they are newer than the provided date.
def get_new_csv(working_dir, edition, last_import_date = None):

    conf = get_geoip_config()

    if edition not in conf['EditionIDs']:
        print(f'We do not appear to be licensed for {edition}.')
        return None

    basic = HTTPBasicAuth(conf['AccountID'], conf['LicenseKey'])

    # Check the headers for the date of the CSV file(s) hosted at MaxMind.
    query_parameters = {'suffix' : 'zip'}
    dl_url = f'https://download.maxmind.com/geoip/databases/{edition}-CSV/download'
    r = requests.head(dl_url, allow_redirects=True, auth=basic, params = query_parameters)

    # the last-modified header is in GMT, but the file is dated some other timezone (US East?),
    # so let's not use it.
    #date_format = '%a, %d %b %Y %H:%M:%S %Z'
    #last_modified = datetime.strptime(r.headers.get('last-modified'), date_format).date()

    # Let's get the date from the filename in the Content-Disposition header
    date_format = '%Y%m%d'
    last_modified = datetime.strptime(r.headers.get('content-disposition').split('_')[1].split('.')[0],date_format).date()

    # If MaxMind has something newer than what's in our DB, then download and extract.
    zipfilename = f'{working_dir}/{edition}-CSV.zip'
    date_suffix = last_modified.strftime('%Y%m%d')
    if not last_import_date or last_modified > last_import_date:
        r = requests.get(dl_url, allow_redirects=True, auth=basic, params = query_parameters)
        with open(zipfilename, mode='wb') as file:
            file.write(r.content)

        with ZipFile(zipfilename, 'r') as zfo:
            base_path = f'{edition}-CSV_{date_suffix}/{edition}'
            zfo.extract(f'{base_path}-Blocks-IPv4.csv', working_dir)
            zfo.extract(f'{base_path}-Blocks-IPv6.csv', working_dir)
            if edition != 'GeoLite2-ASN':
                zfo.extract(f'{base_path}-Locations-{mylocale}.csv', working_dir)
        zfo.close()

        return last_modified

    # Return none if we didn't download anything
    return None

# This function updates data in the database if there is data at Maxmind newer than
# what is in the database.
def update_db(edition):

    tmp_dir = tempfile.TemporaryDirectory()
    working_dir = tmp_dir.name

    newdate = get_new_csv(working_dir, edition, get_latest_import_date(edition))

    if not newdate:
        tmp_dir.cleanup()
        return False

    conn = psycopg2.connect(database=DB_NAME)
    cur = conn.cursor()

    locations_file = None
    date_suffix = newdate.strftime('%Y%m%d')
    base_path = f'{working_dir}/{edition}-CSV_{date_suffix}/{edition}'
    blocks_files = [ f'{base_path}-Blocks-IPv4.csv', f'{base_path}-Blocks-IPv6.csv' ]
    data_sets = { 'network' : blocks_files }
    if edition != 'GeoLite2-ASN':
        data_sets['geoname_id'] = [ f'{base_path}-Locations-{mylocale}.csv' ]

    # There are two CSV files to go through for network data, and one CSV file for location data.
    for data_set in data_sets.keys():

        if data_set == 'network':
            if edition == 'GeoLite2-ASN':
                delete_stmt = 'DELETE FROM geoip2_asn;'
                insert_stmt = 'INSERT INTO geoip2_asn VALUES (%s,%s,%s,%s);'
                idx_del_stmt = None
                idx_create_stmt = None
            else:
                delete_stmt = 'DELETE FROM geoip2_network;'
                insert_stmt = 'INSERT INTO geoip2_network VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
                idx_del_stmt = 'DROP INDEX IF EXISTS geoip2_network_network_idx;'
                idx_create_stmt = 'CREATE INDEX geoip2_network_network_idx ON geoip2_network USING gist (network inet_ops);'
                if edition == 'GeoLite2-City':
                    insert_stmt = 'INSERT INTO geoip2_network VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
                else:
                    insert_stmt = 'INSERT INTO geoip2_network_country VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'

        else:
            if edition == 'GeoLite2-Country':
                # insert into a view of the location table. Country and City can't really co-exist,
                # since the former is a subset of the latter.
                insert_stmt = 'INSERT INTO geoip2_location_country VALUES (%s,%s,%s,%s,%s,%s,%s,%s);'
            else:
                insert_stmt = 'INSERT INTO geoip2_location VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
            delete_stmt = 'DELETE FROM geoip2_location;'
            idx_del_stmt = None
            idx_create_stmt = None


        # Delete stale data before importing new data, unless you want to track history
        if idx_del_stmt:
            cur.execute(idx_del_stmt)
        cur.execute(delete_stmt)

        for f in data_sets[data_set]:
            with open(f) as csvf:
                csvReader = csv.DictReader(csvf)
                for row in csvReader:
                    dbrow = row
                    dbrow['last_modified'] = newdate

                    values = []
                    # Build a list with elements of the correct db data type for insertion
                    for k,v in dbrow.items():
                        if type(v) is str:
                            if v == '':
                                values.append(None)
                            elif v.isnumeric():
                                if '.' in v:
                                    values.append(float(v))
                                else:
                                    if k[:3] == 'is_':
                                        values.append(bool(int(v)))
                                    elif '_iso_' in k:
                                        values.append(v)
                                    else:
                                        values.append(int(v))
                            else:
                                values.append(v)
                        else:
                            values.append(v)

                    cur.execute(insert_stmt,values)

        # Recreate index now that we're done inserting
        if idx_create_stmt:
            cur.execute(idx_create_stmt)

    # if we made it here without throwing an un-caught exception then commit.
    conn.commit()
    conn.close()
    tmp_dir.cleanup()
    return True

if __name__ == '__main__':
    import sys
    supported = [ 'GeoLite2-ASN', 'GeoLite2-City', 'GeoLite2-Country' ]
    editions = []
    for a in sys.argv[1:]:
        if a in supported:
            if a not in editions:
                editions.append(a)
        else:
            print(f"{a} is not supported. Skipping.")

    if 'GeoLite2-City' in editions and 'GeoLite2-Country' in editions:
        print('The Country dataset is a subset of the City dataset, so pick one.');
        quit()

    for e in editions:
        updated = update_db(e)
        if updated:
            print(f'{e}: database updated')
        else:
            print(f'{e}: database not updated')
