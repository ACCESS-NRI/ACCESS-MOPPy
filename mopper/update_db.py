import sqlite3
import json
import sys

def get_rows(conn, exp):
    cursor = conn.cursor()
    cursor.execute("select filename,variable_id,ctable,calculation," 
                   "vin,status,ROWID from filelist where "
                   + f"exp_id='{exp}'")
    rows = cursor.fetchall()
    return rows


def update_status(conn, varid, ctable, old, new):
    cur = conn.cursor()
    cur.execute(f"UPDATE filelist SET status='{new}' where "
                + f"status='{old}' and variable_id='{varid}'"
                + f"and ctable='{ctable}'")
    print(f"Updated {cur.rowcount} rows")
    conn.commit()
    
    return


def update_map(conn, varid, ctable):
    """Read mappings for variable from map file and
    update them in filelist
    """
    keys = ['frequency','realm','timeshot','calculation', 'positive', 'resample']
    keys2 = {'vin': 'input_vars', 'in_units': 'units'}
    fname = f"maps/{ctable}.json"
    with open(fname, 'r') as f:
         data = json.load(f)
    for row in data:
        if row['cmor_var'] == varid and row['cmor_table'] == ctable:
            break
    args = {k: row[k] for k in keys}
    for k,v in keys2.items():
        args[k] = row[v]
    cur = conn.cursor()
    sql = f"UPDATE filelist SET"
    for k,v in args.items(): 
        sql += f" {k}='{v}'," 
    sql = sql[:-1] + f" WHERE variable_id='{varid}' and ctable='{ctable}'" 
    print(sql)
    cur.execute(sql)
    print(f"Updated {cur.rowcount} rows")
    conn.commit()
    return


def get_summary(rows):
    """Get a summary of variables with issues
       by status messages
    """
    status_msg = {'unprocessed': "file ready to be processed",
        'data_unavailable': "incomplete input data", 
        'processed': "file already processed",
        'unknown_return_code': "processing failed with unidentified error",
        'processing_failed': "processing failed with unidentified error",
        'file_mismatch': "produced but file name does not match expected",
        'cmor_error': "cmor variable definition or write failed",
        'mapping_error': "problem with variable mapping and/or definition"}

    flist = {k:set() for k in status_msg.keys()}
    for r in rows:
        if r[5] != 'processed':
            flist[r[5]].add((r[1],r[2],r[3],r[4]))
    for k,value in flist.items():
        if len(value) > 0:
           print(status_msg[k])
           for v in value:
               print(f"Variable {v[0]} - {v[1]};" +
                     f"calculation: {v[2]} with input {v[3]}") 
    return flist


def process_var(conn, flist):
    """For each variable ask if they want ton update status to
    processed or unprocessed
    """
    for k,value in flist.items():
        for v in value:
            print(f"status of {v[0]} - {v[1]} is {k}")
            ans = input("Update status to unprocessed? (Y/N)")
            if ans == 'N':
                ans = input("Update status to processed? (Y/N)")
                if ans == 'Y':
                    update_status(conn, v[0], v[1], k, 'processed')
                else:
                    print(f"No updates for {v[0]}-{v[1]}")
            else:
                update_status(conn, v[0], v[1], k, 'unprocessed')
                ans = input("Update mapping? (Y/N)")
                if ans == 'Y':
                    update_map(conn, v[0], v[1]) 
                
    return


exp = sys.argv[1]
if len(sys.argv) == 3:
    dbname = sys.argv[2]
else:
    dbname = 'mopper.db'
conn=sqlite3.connect(dbname, timeout=200.0)
rows = get_rows(conn, exp)
flist =  get_summary(rows)
process_var(conn, flist)
