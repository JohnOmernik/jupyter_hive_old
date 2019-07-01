#!/usr/bin/python

import requests
import socket
import json
#from getpass import getpass
import sys
import os
import time
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)
from requests.packages.urllib3.exceptions import SubjectAltNameWarning, InsecureRequestWarning
#from requests_toolbelt.adapters import host_header_ssl
#requests.packages.urllib3.disable_warnings(SubjectAltNameWarning)
from collections import OrderedDict
from IPython.core.display import HTML
import pandas as pd
from pyhive import hive as hivemod

try:
    from beakerx import *
    from beakerx.object import beakerx
except:
    pass

#import IPython.display
from IPython.display import display_html, display, Javascript, FileLink, FileLinks, Image
import ipywidgets as widgets

@magics_class
class Hive(Magics):
    # Static Variables
    myip = None
    mysession = None
    hive_connected = False
    hive_pass = ""

    debug = False

    # Variables Dictionary
    hive_opts = {}

    # Option Format: [ Value, Description]

    # Pandas Variables
    hive_opts['pd_display_idx'] = [False, "Display the Pandas Index with output"]
    hive_opts['pd_replace_crlf'] = [True, "Replace extra crlfs in outputs with String representations of CRs and LFs"]
    hive_opts['pd_max_colwidth'] = [50, 'Max column width to display']
    hive_opts['pd_display.max_rows'] = [1000, 'Number of Max Rows']
    hive_opts['pd_display.max_columns'] = [None, 'Max Columns']
    hive_opts['pd_use_beaker'] = [False, 'Use the Beaker system for Pandas Display']
    hive_opts['pd_beaker_bool_workaround'] = [True, 'Look for Dataframes with bool columns, and make it object for display in BeakerX']

    pd.set_option('display.max_columns', hive_opts['pd_display.max_columns'][0])
    pd.set_option('display.max_rows', hive_opts['pd_display.max_rows'][0])
    pd.set_option('max_colwidth', hive_opts['pd_max_colwidth'][0])

    # Get Env items (User and/or Base URL)
    try:
        tuser = os.environ['JUPYTER_USER']
    except:
        tuser = ''
    try:
        turl = os.environ['JUPYTER_HIVE_BASE_URL']
    except:
        turl = ""

    # Hive specific variables
    hive_opts['hive_max_rows'] = [1000, 'Max number of rows to return, will potentially add this to queries']
    hive_opts['hive_user'] = [tuser, "User to connect with hive - Can be set via ENV Var: JPY_USER otherwise will prompt"]
    hive_opts['hive_base_url'] = [turl, "URL to connect to Hive server. Can be set via ENV Var: HIVE_BASE_URL"]
    hive_opts['hive_base_url_host'] = ["", "Hostname of hive connection derived from hive_base_url"]
    hive_opts['hive_base_url_port'] = ["", "Port of hive connection derived from hive_base_url"]
    hive_opts['hive_base_url_scheme'] = ["", "Scheme of hive connection derived from hive_base_url"]
    hive_opts['hive_verbose_errors'] = [False, "Print the whole hive error if True, else try to only print the relavent"]

    # Class Init function - Obtain a reference to the get_ipython()
    def __init__(self, shell, pd_use_beaker=False, *args, **kwargs):
        super(Hive, self).__init__(shell)
        self.myip = get_ipython()
        self.hive_opts['pd_use_beaker'][0] = pd_use_beaker
        if pd_use_beaker == True:
            try:
                beakerx.pandas_display_table()
            except:
                print("WARNING - BEAKER SUPPORT FAILED")

    def retStatus(self):

        print("Current State of Hive Interface:")
        print("")
        print("{: <30} {: <50}".format(*["Connected:", str(self.hive_connected)]))
        print("{: <30} {: <50}".format(*["Debug Mode:", str(self.debug)]))
        print("")
        print("Hive Status URL: http://%s:10002/hiveserver2.jsp" % self.hive_ops['hive_base_url_host'][0])
        print("")
        print("Display Properties:")
        print("-----------------------------------")
        for k, v in self.hive_opts.items():
            if k.find("pd_") == 0:
                try:
                    t = int(v[1])
                except:
                    t = v[1]
                if v[0] is None:
                    o = "None"
                else:
                    o = v[0]
                myrow = [k, o, t]
                print("{: <30} {: <50} {: <20}".format(*myrow))
                myrow = []


        print("")
        print("Hive Properties:")
        print("-----------------------------------")
        for k, v in self.hive_opts.items():
            if k.find("hive_") == 0:
                if v[0] is None:
                    o = "None"
                else:
                    o = str(v[0])
                myrow = [k, o, v[1]]
                print("{: <30} {: <50} {: <20}".format(*myrow))
                myrow = []


    def setvar(self, line):
        pd_set_vars = ['pd_display.max_columns', 'pd_display.max_rows', 'pd_max_colwidth', 'pd_use_beaker']
        allowed_opts = pd_set_vars + ['pd_replace_crlf', 'pd_display_idx', 'hive_base_url', 'hive_verbose_errors']

        tline = line.replace('set ', '')
        tkey = tline.split(' ')[0]
        tval = tline.split(' ')[1]
        if tval == "False":
            tval = False
        if tval == "True":
            tval = True
        if tkey in allowed_opts:
            self.hive_opts[tkey][0] = tval
            if tkey in pd_set_vars:
                try:
                    t = int(tval)
                except:
                    t = tval
                pd.set_option(tkey.replace('pd_', ''), t)
        else:
            print("You tried to set variable: %s - Not in Allowed options!" % tkey)


    def disconnectHive(self):
        if self.hive_connected == True:
            print("Disconnected Hive Session from %s" % self.hive_opts['hive_base_url'][0])
        else:
            print("Hive Not Currently Connected - Resetting All Variables")
        try:
            self.mysession.close()
        except:
            print("Disconnect error: making session None")
        self.mysession = None
        self.hive_connected = False

    def connectHive(self, prompt=False):
        global tpass
        if self.hive_connected == False:
            if prompt == True or self.hive_opts['hive_user'][0] == '':
                print("User not specified in JPY_USER or user override requested")
                tuser = input("Please type user name if desired: ")
                self.hive_opts['hive_user'][0] = tuser
            print("Connecting as user %s" % self.hive_opts['hive_user'][0])
            print("")
            if prompt == True or self.hive_opts['hive_base_url'][0] == '':
                print("HIVE Base URL not specified in HIVE_BASE_URL or override requested")
                turl = input("Please type in the full HIVE URL: ")
                self.hive_opts['hive_base_url'][0] = turl
            print("Connecting to Hive URL: %s" % self.hive_opts['hive_base_url'][0])
            print("")

            myurl = self.hive_opts['hive_base_url'][0]
            ts1 = myurl.split("://")
            self.hive_opts['hive_base_url_scheme'][0] = ts1[0]
            t1 = ts1[1]
            ts2 = t1.split(":")
            self.hive_opts['hive_base_url_host'][0] = ts2[0]
            self.hive_opts['hive_base_url_port'][0] = ts2[1]

            # No hive password at this point, so we are not including it
    #        print("Please enter the password you wish to connect with:")
    #        tpass = ""
    #        self.myip.ex("from getpass import getpass\ntpass = getpass(prompt='Drill Connect Password: ')")
    #        tpass = self.myip.user_ns['tpass']

   #         self.drill_pass = tpass
   #         self.myip.user_ns['tpass'] = ""

            result = self.authHive()
            if result == 0:
                self.hive_connected = True
                print("%s - Hive Connected!" % self.hive_opts['hive_base_url'][0])
            else:
                print("Connection Error - Perhaps Bad Usename/Password?")

        else:
            print("Hive is already connected - Please type %hive for help on what you can you do")

        if self.hive_connected != True:
            self.disconnectHive()

    def authHive(self):
        self.mysession = None
        result = -1
        try:
            # To do, allow settings hive setting from ENV
            self.mysession = hivemod.Connection(host=self.hive_opts['hive_base_url_host'][0], port=self.hive_opts['hive_base_url_port'][0], username=self.hive_opts['hive_user'][0])
            result = 0
        except:
            print("Hive Connection Error!")
            result = -2
        return result

    def runQuery(self, query):
        if query.find(";") >= 0:
            print("WARNING - Do not type a trailing semi colon on queries, your query will fail (like it probably did here)")
        mydf = None
        if self.hive_connected == True:
            starttime = int(time.time())
            try:
                mydf = pd.read_sql(query, self.mysession)
                status = "Success"
            except (TypeError):
                status = "Success - No Results"
                mydf = None
            except Exception as e:
                str_err = str(e)
                if self.hive_opts['hive_verbose_errors'][0] == True:
                    status = "Failure - query_error: " + str_err
                else:
                    msg_find = "errorMessage=\""
                    em_start = str_err.find(msg_find)
                    find_len = len(msg_find)
                    em_end = str_err[em_start + find_len:].find("\"")
                    str_out = str_err[em_start + find_len:em_start + em_end + find_len]
                    status = "Failure - query_error: " + str_out
            endtime = int(time.time())
            query_time = endtime - starttime
        else:
            mydf = None
            query_time = 0
            status = "Hive Not Connected"

        return mydf, query_time, status


    def displayHelp(self):
        print("jupyter_hive is a interface that allows you to use the magic function %hive to interact with an Apache Hive installation.")
        print("")
        print("jupyter_hive has two main modes %hive and %%hive")
        print("%hive is for interacting with a Hive installation, connecting, disconnecting, seeing status, etc")
        print("%%hive is for running queries and obtaining results back from the Hive cluster")
        print("")
        print("%hive functions available")
        print("###############################################################################################")
        print("")
        print("{: <30} {: <80}".format(*["%hive", "This help screen"]))
        print("{: <30} {: <80}".format(*["%hive status", "Print the status of the Hive connection and variables used for output"]))
        print("{: <30} {: <80}".format(*["%hive connect", "Initiate a connection to the Hive cluster, attempting to use the ENV variables for Hive URL and Hive Username"]))
        print("{: <30} {: <80}".format(*["%hive connect alt", "Initiate a connection to the Hive cluster, but prompt for Username and URL regardless of ENV variables"]))
        print("{: <30} {: <80}".format(*["%hive disconnect", "Disconnect an active Hive connection and reset connection variables"]))
        print("{: <30} {: <80}".format(*["%hive set %variable% %value%", "Set the variable %variable% to the value %value%"]))
        print("{: <30} {: <80}".format(*["%hive debug", "Sets an internal debug variable to True (False by default) to see more verbose info about connections"]))
        print("")
        print("Running queries with %%hive")
        print("###############################################################################################")
        print("")
        print("When running queries with %%hive, %%hive will be on the first line of your cell, and the next line is the query you wish to run. Example:")
        print("")
        print("%%hive")
        print("select * from `mydatabase`.`mytable`")
        print("")
        print("Some query notes:")
        print("- If the number of results is less than pd_display.max_rows, then the results will be diplayed in your notebook")
        print("- You can change pd_display.max_rows with %hive set pd_display.max_rows 2000")
        print("- The results, regardless of display will be place in a Pandas Dataframe variable called prev_hive")
        print("- prev_hive is overwritten every time a successful query is run. If you want to save results assign it to a new variable")



    @line_cell_magic
    def hive(self, line, cell=None):
        if cell is None:
            line = line.replace("\r", "")
            if line == "":
                self.displayHelp()
            elif line.lower() == "status":
                self.retStatus()
            elif line.lower() == "debug":
                print("Toggling Debug from %s to %s" % (self.debug, not self.debug))
                self.debug = not self.debug
            elif line.lower() == "disconnect":
                self.disconnectHive()
            elif line.lower() == "connect alt":
                self.connectHive(True)
            elif line.lower() == "connect":
                self.connectHive(False)
            elif line.lower() .find('set ') == 0:
                self.setvar(line)
            else:
                print("I am sorry, I don't know what you want to do, try just %hive for help options")
        else:
            cell = cell.replace("\r", "")
            if self.hive_connected == True:
                result_df, qtime, status = self.runQuery(cell)
                if status.find("Failure") == 0:
                    print("Error: %s" % status)
                elif status.find("Success - No Results") == 0:
                    print("No Results returned in %s seconds" % qtime)
                else:
                   self.myip.user_ns['prev_hive'] = result_df
                   mycnt = len(result_df)
                   print("%s Records in Approx %s seconds" % (mycnt,qtime))
                   print("")

                   if mycnt <= int(self.hive_opts['pd_display.max_rows'][0]):
                       if self.debug:
                           print("Testing max_colwidth: %s" %  pd.get_option('max_colwidth'))
                       if self.hive_opts['pd_use_beaker'][0] == True:
                           if self.hive_opts['pd_beaker_bool_workaround'][0]== True:
                                for x in result_df.columns:
                                    if result_df.dtypes[x] == 'bool':
                                        result_df[x] = result_df[x].astype(object)
                           display(TableDisplay(result_df))
                       else:
                           display(HTML(result_df.to_html(index=self.hive_opts['pd_display_idx'][0])))
                   else:
                       print("Number of results (%s) greater than pd_display_max(%s)" % (mycnt, self.hive_opts['pd_display.max_rows'][0]))


            else:
                print("Hive is not connected: Please see help at %hive  - To Connect: %hive connect")


    #Display Only functions


    def replaceHTMLCRLF(self, instr):
        gridhtml = instr.replace("<CR><LF>", "<BR>")
        gridhtml = gridhtml.replace("<CR>", "<BR>")
        gridhtml = gridhtml.replace("<LF>", "<BR>")
        gridhtml = gridhtml.replace("&lt;CR&gt;&lt;LF&gt;", "<BR>")
        gridhtml = gridhtml.replace("&lt;CR&gt;", "<BR>")
        gridhtml = gridhtml.replace("&lt;LF&gt;", "<BR>")
        return gridhtml

