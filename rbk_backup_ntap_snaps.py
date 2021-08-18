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
import urllib3
urllib3.disable_warnings()

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

def create_fs_template(rubrik, ntap_host, share):
    if share.startswith('/'):
        payload = [{"includes": ["x"], "excludes": [".snapshot"], "name": ntap_host + '_' + share[1:], "shareType": "NFS",
                    "allowBackupHiddenFoldersInNetworkMounts": True}]
    else:
        payload = [{"includes": ["x"], "excludes": ["~snapshot"], "name": ntap_host + '_' + share, "shareType": "SMB"}]
    fst_data = rubrik.post('internal', '/fileset_template/bulk', payload, timeout=timeout)
    if fst_data['total'] == 0:
        sys.stderr.write("Error Creating Fileset Template: " + ntap_host + '_' + share)
        exit(1)
    return(str(fst_data['data'][0]['id']))

def get_fsid(id, data):
    if data['total'] == 0:
        return("")
    for fs in data['data']:
        if fs['templateId'] == id:
            return(fs['id'])
    return("")

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
    admin_lif = ""
    INTERACTIVE = True
    timeout = 60
    fileset = ""
    sla= ""
    NAS_DA = False

    optlist, args = getopt.getopt(sys.argv[1:], 'hDn:c:t:p:ya:f:s:d', ['--help', '--DEBUG', '--creds=', '--ntap_creds=', '--token='
                                  '--pattern=', '--yes', '--admin=', '--sla=', '--nas_da'])
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
        if opt in ('-a', '--admin'):
            admin_lif = a
        if opt in ('-s', '--sla'):
            sla = a
        if opt in ('d', '--nas_da'):
            NAS_DA = True

    try:
        (ntap_host, rubrik_host, volume, share, outfile) = args
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

    if not admin_lif:
        admin_lif = ntap_host
    netapp = NaServer(admin_lif, 1, 130)
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
    api = NaElement('snapshot-list-info')
    api.child_add_string("volume", volume)
    result = netapp.invoke_elem(api)
    ntap_invoke_err_check(result)
    dprint(result.sprintf())
    snaps = result.child_get('snapshots').children_get()
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
    hs_data = rubrik.get('internal', '/host/share', timeout=timeout)
    hs_id = ""
    for hs in hs_data['data']:
        if hs['hostname'] == ntap_host and hs['exportPoint'] == share:
            hs_id = str(hs['id'])
            break
    if not hs_id:
        sys.stderr.write("Can't find share: " + ntap_host + ':' + share + '\n')
        exit(1)
    dprint("HS_ID: " + hs_id)
    if share.startswith('/'):
        fst_ck = share[1:]
        protocol = "NFS"
    else:
        fs_ck = share
        protocol = "SMB"
    fs_data = rubrik.get('v1', '/fileset?share_id=' + hs_id, timeout=timeout)
    if fs_data['total'] == 0:
        fst_data = rubrik.get('v1', '/fileset_template?name=' + ntap_host + '_' + fst_ck + '&share_type=' + protocol,
                              timeout=timeout)
        if fst_data['total'] == 0:
            print("No fileset found...creating template" + ntap_host + '_' + fst_ck)
            fst_id = create_fs_template(rubrik, ntap_host, share)
        else:
            fst_id = str(fst_data['data'][0]['id'])
        print('Adding fileset template ' + ntap_host + '_' + share + ' to share')
        payload = {'shareId': hs_id, 'templateId': fst_id, 'isPassthrough': NAS_DA}
        fst_add = rubrik.post('v1', '/fileset', payload, timeout=timeout)
        fs_id = str(fst_add['id'])
    else:
        valid = False
        while not valid:
            print('Found multiple filesets on the share.  Choose an existing or create a new one:\n')
            for i, f in enumerate(fs_data['data']):
                print(str(i) + ': ' + f['name'] + '  [' + f['configuredSlaDomainName'] + ']')
            print('\nN: Create a new fileset\n')
            fs_index = python_input("Selection: ")
            if fs_index == "N" or fs_index == "n":
                fst_data = rubrik.get('v1','/fileset_template?name=' + ntap_host + '_' + fst_ck + '&share_type=' + protocol,
                                      timeout=timeout)
                if fst_data['total'] == 0:
                    fst_id = create_fs_template(rubrik, ntap_host, share)
                else:
                    fst_id = fst_data['data'][0]['id']
                print('Adding fileset template ' + ntap_host + '_' + share + ' to share')
                payload = {'shareId': hs_id, 'templateId': fst_id, 'isPassthrough': NAS_DA}
                fst_add = rubrik.post('v1', '/fileset', payload, timeout=timeout)
                fs_id = str(fst_add['id'])
                valid = True
            elif int(fs_index) in range(0, len(fs_data['data'])):
                fs_id = str(fs_data['data'][int(fs_index)]['id'])
                valid = True
    dprint("FS_ID: " + fs_id)
    if sla:
        sla_data = rubrik.get('v2', '/sla_domain?name=' + sla, timeout=timeout)
        if sla_data['total'] == 0:
            sys.stderr.write('SLA Domain ' + sla + ' not found.\n')
            exit(2)
        elif sla_data['total'] != 1:
            sys.stderr.write("Multiple SLA domains found. The script needs one\n")
            exit(2)
        else:
            sla_id = sla_data['data'][0]['id']
    else:
        sla_id = fs_data['data'][int(fs_index)]['configuredSlaDomainId']
        if sla_id == "UNPROTECTED":
            sys.stderr.write("Fileset assigned as no SLA.  Use -s to define one.\n")
            exit(2)
    dprint("SLA_ID: " + sla_id)