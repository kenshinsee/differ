from file_differ import File_Differ
from optparse import OptionParser
from subprocess import Popen, PIPE
import os, sys, datetime, yaml, pyodbc
import pandas as pd

def logger(info):
    now = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    print("[%s] %s" % (now, info))

def vertica_export(file_a_b, meta):
    vertica_exp_cmd = '''vsql -h %(db_server)s -d %(db_name)s -U %(username)s -w %(password)s -o %(out_file)s -c "%(source_query)s" -F "%(delimiter_opt)s" -Atq -P footer=off -q ''' % { 
                'db_server': meta[file_a_b]["query_info"]["db_server"], 
                'db_name': meta[file_a_b]["query_info"]["db_name"], 
                'username': meta[file_a_b]["query_info"]["username"], 
                'password': meta[file_a_b]["query_info"]["password"], 
                'out_file': meta[file_a_b]["file_loc"].replace("\\", "\\\\"), 
                'source_query': meta[file_a_b]["query_info"]["sql"].replace('$schema_name', meta[file_a_b]["query_info"]["schema_name"]),
                'delimiter_opt': ","
            }
    return vertica_exp_cmd
    
def sqlserver_export(file_a_b, meta):
    sql_exp_cmd = '''bcp "%(source_query)s" queryout "%(out_file)s" -c -t "%(delimiter_opt)s" -S "%(db_server)s" -d "%(db_name)s" -U "%(username)s" -P "%(password)s" ''' % { 
                'db_server': meta[file_a_b]["query_info"]["db_server"], 
                'db_name': meta[file_a_b]["query_info"]["db_name"], 
                'username': meta[file_a_b]["query_info"]["username"], 
                'password': meta[file_a_b]["query_info"]["password"], 
                'out_file': meta[file_a_b]["file_loc"], 
                'source_query': meta[file_a_b]["query_info"]["sql"],
                'delimiter_opt': ","
            }
    return sql_exp_cmd
       
    
# Input parameters
parser = OptionParser()
parser.add_option("--mode", "-m", dest="mode", action="store", type="string", default="exportAndCompare")
parser.add_option("--config", "-c", dest="config", action="store", type="string", default="test.yml")
parser.add_option("--sheets", "-s", dest="sheets", action="store", type="string", default="Summary|aFile|bFile|aOnly|bOnly|Detail|notAllMatch|Agg|notInTolerance")
(options, args) = parser.parse_args()

# Verify input mode
modes = ["export", "compare", "exportAndCompare"]
if not options.mode in modes:
    raise ValueError("%s is an invalid mode." % options.mode)

# Get file path
curr_path = os.path.split( os.path.realpath( sys.argv[0] ) )[0]
conf_path = "%s\\%s" %(curr_path, "config")
conf_name = "%s\\%s" %(conf_path, options.config)
logger("Loading %s" % conf_name)

# Load config
config_handler = open(conf_name)
meta = yaml.load(config_handler)
config_handler.close()


#-----------------------------------------#
#         Export Data from DB             #
#-----------------------------------------#
if options.mode in ["export", "exportAndCompare"]:
    
    from libs.db.DBOperations import AppAccessLayer
    from libs.db.DBOperations import DWAccessLayer
    from libs.db.Config import Config
    
    file_a_b = ["file_a", "file_b"]

    for file in file_a_b:
        db_type = meta[file]["query_info"]["db_type"] 
        
        # update meta with db_server/db_name/etc from core_config
        if "silo_server" in meta[file]["query_info"].keys() and meta[file]["query_info"]["silo_server"] != None:
            if "db_server" in meta[file]["query_info"].keys() and meta[file]["query_info"]["db_server"] != None:
                # supposed db connection info are specificed in yaml file
                pass
            else:
                try: 
                    db_conn_key_dict = {
                        "vertica": {
                            "dw.server.name": "db_server", 
                            "dw.db.name": "db_name", 
                            "dw.user.id": "username", 
                            "dw.user.password": "password", 
                            "dw.schema.name": "schema_name"
                        }, 
                        "sqlserver": {
                            "etl.fusion.servername": "db_server", 
                            "etl.fusion.databasename": "db_name", 
                            "etl.fusion.userid": "username", 
                            "etl.fusion.password": "password"
                        }
                    }
                    app_conn = AppAccessLayer(db_name = meta[file]["query_info"]["silo_db_name"], rdp_server = meta[file]["query_info"]["silo_server"])
                    config = Config(app_connection = app_conn, silo_id = meta[file]["query_info"]["silo_id"])
                    for name in db_conn_key_dict[db_type].keys():
                        db_key = db_conn_key_dict[db_type][name]
                        logger("%s : %s" % (db_key, config.get_config([name])[name]))
                        if db_key in meta[file]["query_info"].keys():
                            meta[file]["query_info"][db_key] = config.get_config([name])[name]
                        else:
                            meta[file]["query_info"].__setitem__(db_key, config.get_config([name])[name])
                except:
                    raise
                finally: 
                    app_conn.close_connection()
        
        if db_type == "vertica":
            exp_cmd = vertica_export(file, meta)
        elif db_type == "sqlserver":        
            exp_cmd = sqlserver_export(file, meta)
        else:
            raise ValueError("Unsupported db_type %s" % meta[file]["query_info"]["db_type"])

        logger(exp_cmd)
        process = Popen(exp_cmd, shell=True, stdout=PIPE, stderr=PIPE)
        process.wait()

        if process.returncode != 0:
            raise RuntimeError('Command failed, exiting...')

        logger('\n' + process.communicate()[0].decode(encoding='utf-8'))


#-----------------------------------------#
#         Diff File and Export            #
#-----------------------------------------#
if options.mode in ["compare", "exportAndCompare"]:
    
    all_sheets = ["Summary", "aFile", "bFile", "aOnly", "bOnly", "Detail", "notAllMatch", "Agg", "notInTolerance"]
    export_sheets = options.sheets.split("|")
    
    sheets = {
        "Summary": {
            "log": "Writing into summary...", 
            "create_df": "summary_df = fd.get_summary()", 
            "export": 'summary_df.to_excel(writer, "Summary")', 
        }, 
        "aFile": {
            "log": "Writing into aFile...", 
            "create_df": "df_a = fd.get_df_a()", 
            "export": 'df_a.to_excel(writer, "aFile")', 
        }, 
        "bFile": {
            "log": "Writing into bFile...", 
            "create_df": "df_b = fd.get_df_b()", 
            "export": 'df_b.to_excel(writer, "bFile")', 
        }, 
        "aOnly": {
            "log": "Writing into aOnly...", 
            "create_df": "a_only_df = fd.get_a_only()", 
            "export": 'a_only_df.to_excel(writer, "aOnly")', 
        }, 
        "bOnly": {
            "log": "Writing into bOnly...", 
            "create_df": "b_only_df = fd.get_b_only()", 
            "export": 'b_only_df.to_excel(writer, "bOnly")', 
        }, 
        "notAllMatch": {
            "log": "Writing into notAllMatch...", 
            "create_df": "not_all_match = fd.get_df_not_match()", 
            "export": 'not_all_match.to_excel(writer, "notAllMatch")', 
        }, 
        "Detail": {
            "log": "Writing into Detail...", 
            "create_df": "df = fd.get_dataframe()", 
            "export": 'df.to_excel(writer,"Detail")', 
        }, 
        "notInTolerance": {
            "log": "Writing into notInTolerance...", 
            "create_df": "not_in_tolerance = fd.get_agg_not_in_tolerance()", 
            "export": 'not_in_tolerance.to_excel(writer, "notInTolerance")', 
        }, 
        "Agg": {
            "log": "Writing into Agg...", 
            "create_df": "agg_df = fd.get_agg_dataframe()", 
            "export": 'agg_df.to_excel(writer,"Agg")', 
        }, 
    }
    
    fd = File_Differ(meta)
    
    logger("Saving data into excel...")
    writer = pd.ExcelWriter(meta["result_excel"])
    
    for sheet in all_sheets:
        if sheet in export_sheets:
            logger(sheets[sheet]["log"])
            exec(sheets[sheet]["create_df"])
            exec(sheets[sheet]["export"])
    
    writer.save()
    writer.close()


logger("Done")


