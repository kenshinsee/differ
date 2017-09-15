#import scripts.common.RSiCrypto as Cr
import libs.db.RSiCrypto as Cr

class Config:
    def __init__(self, app_connection, silo_id):
        self.__app_connection = app_connection
        self.__silo_id = silo_id
        self.__configs = self.__get_rdp_config()

    def __get_rdp_config(self):
        if self.__app_connection:
            command = "select name, dbo.fn$RSI_get_config_property(name) value " \
                      "from RSI_CORE_CFGPROPERTY WHERE SILO_ID='{0}'".format(self.__silo_id)
            configs = self.__app_connection.query_with_result(command)

            whole_config = {}
            for silo_config in configs:
                if silo_config["name"] in ["dw.user.password", "etl.app.user.password"]:
                    crypto = Cr.Crypto()
                    whole_config[silo_config["name"]] = crypto.decrypt(silo_config["value"])
                else:
                    whole_config[silo_config["name"]] = silo_config["value"]

            return whole_config

    # argument list is a set of arguments based on the query will be filtered
    def get_config(self, argument_list=None):
        if argument_list:
            silo_configs = {argument: self.__configs.get(argument) for argument in argument_list}
            return silo_configs
        else:
            return self.__configs

    def __getitem__(self, name):
        return self.__configs.get(name)

    def get_config_synced(self):
        pass

