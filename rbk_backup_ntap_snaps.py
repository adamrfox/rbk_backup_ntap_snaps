#!/usr/bin/python

from __future__ import print_function
import sys
from datetime import datetime
import pytz
import getopt
import getpass
import rubrik_cdm
sys.path.append('./NetApp')
from NaServer import *
import ssl
import re

def usage():
    print("Usage goes here!")
    exit(0)

def dprint(message):
    if DEBUG:
        print(message)

def python_input(message):
    if int(sys.version[0]) > 2:
        val = input(message)
    else:
        val = raw_input(message)
    return(val)

def ntap_set_err_check(out):
    if(out and (out.results_errno() != 0)) :
        r = out.results_reason()
        print("Connection to filer failed" + r + "\n")
        sys.exit(2)

def ntap_invoke_err_check(out):
    if(out.results_status() == "failed"):
            print(out.results_reason() + "\n")
            sys.exit(2)

def purge_snap_list (snap_list, pattern):
    new_snap_list = []
    dprint("PATTERN: " + pattern)
    for snap in snap_list:
        match = re.search(pattern, snap['name'])
        dprint (snap['name'] + " : " + str(match))
        if match:
            new_snap_list.append({'name': snap['name'], 'time': snap['time']})
    return(new_snap_list)

def get_index_list(s, snap_list):
    index_list = []
    if s.lower() == "all":
        for si in range(0, len(snap_list)):
            index_list.append(si)
    else:
        ilf = s.split(',')
        for f in ilf:
            if '-' in f:
                ff = f.split('-')
                for x in range(int(ff[0]), int(ff[1])+1):
                    index_list.append(x)
            elif f.isdigit():
                index_list.append(f)
            else:
                return([])
    return(index_list)


if __name__ == "__main__":
    ntap_user = ""
    ntap_password = ""
    user = ""
    password = ""
    token = ""
    svm = ""
    DEBUG = False
    snap_list = []
    pattern = ""
    INTERACTIVE = True

    optlist, args = getopt.getopt(sys.argv[1:], 'hDn:c:t:p:y', ['--help', '--DEBUG', '--creds=', '--ntap_creds=', '--token='
                                  '--pattern=', '--yes'])
    for opt, a in optlist:
        if opt in ('-h', '--help'):
            usage()
        if opt in ('-D', '--DEBUG'):
            DEBUG = True
        if opt in ('-c', '--creds'):
            (user, password) = a.split(':')
        if opt in ('-n', '--ntap_creds'):
            (ntap_user, ntap_password) = a.split(':')
        if opt in ('-t', '--token'):
            token = a
        if opt in ('-p', '--pattern'):
            pattern = a
        if opt in ('-y', '--yes'):
            INTERACTIVE = False

    try:
        (ntap_host, rubrik_host, svm, volume, outfile) = args
    except:
        usage()
    if not ntap_user:
        ntap_user = python_input("NTAP SVM User: ")
    if not ntap_password:
        ntap_password = getpass.getpass("NTAP SVM Password: ")
    if not token:
        if not user:
            user = python_input("Rubrik User: ")
        if not password:
            password = getpass.getpass("Rubrik Password: ")
        rubrik = rubrik_cdm.Connect(rubrik_host, user, password)
    else:
        rubrik = rubrik_cdm.Connect(rubrik_host, api_token=token)

    try:
        _create_unverified_https_context = ssl._create_unverified_context
    except AttributeError:
        pass
    else:
        ssl._create_default_https_context = _create_unverified_https_context

    netapp = NaServer(ntap_host, 1, 130)
    out = netapp.set_transport_type('HTTPS')
    ntap_set_err_check(out)
    out = netapp.set_style('LOGIN')
    ntap_set_err_check(out)
    out = netapp.set_admin_user(ntap_user, ntap_password)
    ntap_set_err_check(out)
    api = NaElement('clock-get-timezone')
    result = netapp.invoke_elem(api)
    ntap_invoke_err_check(result)
    ntap_timezone = result.child_get_string('timezone')
    dprint("NTAP_TZ = " + ntap_timezone)
    filer_tz = pytz.timezone(ntap_timezone)
    api = NaElement('snapshot-get-iter')
    xi = NaElement('desired-attributes')
    api.child_add(xi)
    xi1 = NaElement('snapshot-info')
    xi.child_add(xi1)
    xi1.child_add_string('name', '<name>')
    xi1.child_add_string('access-time', '<access-time>')
    xi2 = NaElement('query')
    api.child_add(xi2)
    xi3 = NaElement('snapshot-info')
    xi2.child_add(xi3)
    xi3.child_add_string('vserver', svm)
    xi3.child_add_string('volume', volume)
    api.child_add_string("max-records", 1024)
    result = netapp.invoke_elem(api)
    ntap_invoke_err_check(result)
    dprint(result.sprintf())
    snaps = result.child_get('attributes-list').children_get()
    for s in snaps:
        name = s.child_get_string('name')
        time = s.child_get_string('access-time')
        time_dt = datetime.fromtimestamp(int(time), filer_tz)
        snap_list.append({'name': name, 'time': str(time_dt)[:-6]})
    if pattern:
        snap_list = purge_snap_list(snap_list, pattern)
        if not snap_list:
            sys.stderr.write("Pattern yielded no results\n")
            exit(1)
    dprint(snap_list)
    if INTERACTIVE:
        print("NTAP Snapshots:\n")
        for i, s in enumerate(snap_list):
            print(str(i) + ': ' + s['name'] + '\t\t' + str(s['time']))
    index_list_s = python_input("Select snapshots to backup: ")
    index_list = get_index_list(index_list_s, snap_list)
    dprint("INDEX_LIST = " + str(index_list))
