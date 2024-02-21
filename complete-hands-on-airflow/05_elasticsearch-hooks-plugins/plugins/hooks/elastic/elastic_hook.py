from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base import BaseHook  # all hooks in AF inherit BaseHook Class

from elasticsearch import Elasticsearch


# create a class that inherits from BaseHook
class ElasticHook(BaseHook):
    def __init__(self, conn_id='elastic_default', *args, **kwargs):
        super().__init__(*args, **kwargs)  # BaseHook class
        conn = self.get_connection(conn_id)  # grab information with get_connection method

        conn_config = {}
        hosts = []

        # if have a host
        if conn.host:
            hosts = conn.host.split(',')  # grab the host, split in case of multiple hosts

        # if have a port (in this case, we set 9200)
        if conn.port:
            conn_config['port'] = int(conn.port)  # set in the dict

        # if have a login
        if conn.login:
            conn_config['http_auth'] = (conn.login, conn.password)  # set the tuple

        # initializing the hook
        self.es = Elasticsearch(hosts, **conn_config)  # **conn_config because maybe it hasn't all attributes grabbed before
        self.index = conn.schema

    # it only returns the information about Elasticsearch instance
    def info(self):
        return self.es.info()
    
    # add data to a specific index
    def add_doc(self, index, doc_type, doc):
        self.set_index(index)
        res = self.es.index(index=index, doc_type=doc_type, doc=doc)
        return res

# create a class that inherits from AirflowPlugin, to specify the plugins
class AirflowElasticPlugin(AirflowPlugin):
    name = 'elastic'
    hooks = [ElasticHook]