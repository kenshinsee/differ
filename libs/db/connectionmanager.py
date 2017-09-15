from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import xml.etree.ElementTree as Et
import os

cwd = os.path.dirname(os.path.realpath(__file__))

_settings_file = cwd + "\settings.xml"


_settings_file = cwd + "\settings.xml"


class Connections:
    base = declarative_base()

    # Constructor to define the connection strings and db engine
    def __init__(self, rdp_server=None, db_name=None):
        if rdp_server is not None:
            driver = "SQL+Server"
        else:
            self.config_db_connection_string = {}
            # declarative base to be used while defining tables
            _cfg_environment = Et.parse(_settings_file).getroot()
            for _environment in _cfg_environment.iter("Environment"):
                _env_name = _environment.get("name")

            for _connection in _cfg_environment.findall('Connection[@EnvironmentName="' + _env_name + '"]'):
                for _config_db in _connection:
                    self.config_db_connection_string["Driver"] = _config_db.find('Driver').text
                    self.config_db_connection_string["Server"] = _config_db.find('Server').text
                    self.config_db_connection_string["IP"] = _config_db.find('IP').text
                    self.config_db_connection_string["DatabaseInstance"] = _config_db.find('DatabaseInstance').text
                    self.config_db_connection_string["InitialCatalog"] = _config_db.find('InitialCatalog').text
                    self.config_db_connection_string["Port"] = _config_db.find('Port').text
                    self.config_db_connection_string["TrustedConnection"] = _config_db.find('TrustedConnection').text
                    self.config_db_connection_string["UserID"] = _config_db.find('UserID').text
                    self.config_db_connection_string["Password"] = _config_db.find('Password').text

            rdp_server = self.config_db_connection_string["Server"]
            db_name = self.config_db_connection_string["InitialCatalog"]
            driver = self.config_db_connection_string["Driver"]

        self.config_db_engine=create_engine('mssql://' + rdp_server+ '/' +
                                            db_name + '?driver=' +
                                            driver)