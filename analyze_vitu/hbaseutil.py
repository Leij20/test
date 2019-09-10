# -*- coding: UTF-8 -*-
import happybase
import config

hb_connection = None
def get_connect(host=config.hb_host, port=config.hb_port):
    global hb_connection
    if hb_connection is None:
        hb_connection = happybase.Connection(host, port=port)
    return hb_connection

def get_hb_table(table_name):
    connection = get_connect()
    table = connection.table(table_name)
    return table

def write_table(table, key, data):
    row_key = str.encode(key)
    table.put(row_key, data)
