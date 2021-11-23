#!/usr/bin/python

from __future__ import print_function
import sys
from datetime import datetime
import pytz
import time
import getopt
import getpass
import rubrik_cdm
sys.path.append('./NetApp')
from NaServer import *
import ssl
import re
import xmltodict
import urllib3
urllib3.disable_warnings()

def usage():
    sys.stderr.write('rbk_backup_ntap_snaps.py [-hDd] [-c creds] [-n ntap_creds] [-t token] [-p pattern] [-a admin_lif] [-s sla] ntap rubrik [volume] share log_file\n')
    sys.stderr.write('-h | --help : Prints this help\n')
    sys.stderr.write('-D | -- DEBUG : Debug output.  Only useful for troubleshooting.\n')
    sys.stderr.write('-d | --nas_da : Use NAS DA [default: False]\n')
    sys.stderr.write('-c | --creds= : Specify Rurbik credentials [user:password]\n')
    sys.stderr.write('-n | --ntap_creds= : Specify NTAP credentials [user:password]\n')
    sys.stderr.write('-t | --token= : Specify API token for Rubrik\n')
    sys.stderr.write('-p | --pattern= : Specify a pattern for NTAP snapshot name\n')
    sys.stderr.write('-a | --admin= : Specify an SVM admin LIF if needed.\n')
    sys.stderr.write('-s | --sla= : Specify an SLA.  Use if not using an existing fileset with one assigned.\n')
    sys.stderr.write('ntap : Name or IP of SVM where the shares exist (must match NAS host name on Rubrik)\n')
    sys.stderr.write('rubrik : Name or IP of Rubrik cluster\n')
    sys.stderr.write("volume : Volume name on the NTAP. Only needed if it can't be discoverd from the share name\n")
    sys.stderr.write('share : Share name or export path to be backed up\n')
    sys.stderr.write('log_file : Name of the log file\n')
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

def get_share_config(share_name, xml):
    share_config = {}
    share_data = xmltodict.parse(xml)
    for sh in share_data['results']['attributes-list']['cifs-share']:
        if sh['share-name'] != share_name:
            continue
        prop_list = []
        for p in sh['share-properties']['cifs-share-properties']:
            prop_list.append(p)
        share_config = {'name': sh['share-name'], 'properties': prop_list}
        break
    return(share_config)


def discover_volume(netapp, share):
    dprint("NTAP: " + str(netapp) + " // SHARE: " + str(share))
    if share.startswith('/'):
        api = NaElement('volume-get-iter')
        xi = NaElement('desired-attributes')
        api.child_add(xi)
        xi1 = NaElement('volume-attributes')
        xi.child_add(xi1)
        xi11 = NaElement('volume-id-attributes')
        xi1.child_add(xi11)
        xi11.child_add_string('name', '<name>')
        xi11.child_add_string('junction-path', '<junction-path>')
        result = netapp.invoke_elem(api)
        ntap_invoke_err_check(result)
        vol_list = result.child_get('attributes-list')
        vols = vol_list.child_get('volume-attributes').children_get()
        for v in vols:
            vol_name = v.child_get_string('name')
            vol_path = v.child_get_string('junction-path')
            dprint("VOL_NAME: " + str(vol_name) + " // VOL_PATH: " + str(vol_path))
            if share.startswith(str(vol_path)):
                return(vol_name)
    else:
        api = NaElement('cifs-share-get-iter')
        xi = NaElement('desired-attributes')
        api.child_add(xi)
        xi1 = NaElement('cifs-share')
        xi.child_add(xi1)
        xi1.child_add_string('share-name', '<share-name>')
        xi1.child_add_string('volume', '<volume>')
        result = netapp.invoke_elem(api)
        ntap_invoke_err_check(result)
#        print(result.sprintf())
        share_list = result.child_get('attributes-list').children_get()
        for sh in share_list:
            share_name = sh.child_get_string('share-name')
            if share_name == share:
                vol_name = sh.child_get_string('volume')
                return(vol_name)
    return("")

def update_share_config(netapp, share_config):
    api = NaElement('cifs-share-modify')
    api.child_add_string('share-name', share_config['name'])
    xi = NaElement('share-properties')
    api.child_add(xi)
    for p in share_config['properties']:
        xi.child_add_string('cifs-share-properties', p)
    result = netapp.invoke_elem(api)
    ntap_invoke_err_check(result)
    return(share_config)

def update_share_path(rubrik, id, snap_name):
    hs_data = rubrik.get('internal', '/host/share/' + id, timeout=timeout)
    path = hs_data['exportPoint']
    if '.snapshot/' in path:
        path_list = path.split('/')
        for i, pe in enumerate(path_list):
            if pe == '.snapshot':
                path_list[i+1] = snap_name
                break
        path = '/'.join(path_list)
    else:
        path = path + '/.snapshot/' + snap_name
    return (path)

def update_smb_path(netapp, share, snap_name):
    share_path = get_share_path(netapp, share)
    if '.snapshot/' in share_path:
        path_list = share_path.split('/')
        for i, pe, in enumerate(path_list):
            if pe == ".snapshot":
                path_list[i+1] = snap_name
                break
        share_path = '/'.join(path_list)
    else:
        share_path = share_path +'/.snapshot/' + snap_name
    api = NaElement('cifs-share-modify')
    api.child_add_string('share-name', share)
    api.child_add_string('path', share_path)
    result = netapp.invoke_elem(api)
    ntap_invoke_err_check(result)
    dprint("SMB path updated to " + share_path)
    return

def share_exists(netapp, share_name):
    api = NaElement('cifs-share-get-iter')
    xi = NaElement('desired-attributes')
    api.child_add(xi)
    xi1 = NaElement('cifs-share')
    xi.child_add(xi1)
    xi1.child_add_string('share-name', '<share_name>')
    api.child_add_string('max-records', 5000)
    result = netapp.invoke_elem(api)
    ntap_invoke_err_check(result)
    dprint(result.sprintf())
    share_list = result.child_get('attributes-list').children_get()
    for sh in share_list:
        sh_name = sh.child_get_string('share-name')
        if sh_name == share_name:
            return(True)
    return(False)

def get_share_path(netapp, share_name):
    sh_path = ""
    api = NaElement('cifs-share-get-iter')
    xi = NaElement('desired-attributes')
    api.child_add(xi)
    xi1 = NaElement('cifs-share')
    xi.child_add(xi1)
    xi1.child_add_string('share-name', '<share_name>')
    xi1.child_add_string('path', '<path')
    api.child_add_string('max-records', 5000)
    result = netapp.invoke_elem(api)
    ntap_invoke_err_check(result)
    share_list = result.child_get('attributes-list').children_get()
    for sh in share_list:
        sh_name = sh.child_get_string('share-name')
        if sh_name == share_name:
            sh_path = sh.child_get_string('path')
            break
    return(sh_path)

def temp_share(cmd, netapp, share, domain, user):
    if cmd == "create":
        if not share_exists(netapp, share + '_temp$'):
            share_path = get_share_path(netapp, share)
            api = NaElement('cifs-share-create')
            api.child_add_string('path', share_path)
            api.child_add_string('share-name', share + '_temp$')
            xi = NaElement('share-properties')
            api.child_add(xi)
            xi.child_add_string('cifs-share-properties', 'showsnapshot')
            xi.child_add_string('cifs-share-properties', 'oplocks')
            xi.child_add_string('cifs-share-properties', 'changenotify')
            result = netapp.invoke_elem(api)
            ntap_invoke_err_check(result)
            api = NaElement('cifs-share-access-control-delete')
            api.child_add_string('share', share + '_temp$')
            api.child_add_string('user-group-type', 'windows')
            api.child_add_string('user-or-group', 'Everyone')
            result = netapp.invoke_elem(api)
            ntap_invoke_err_check(result)
            smb_user = domain + '\\' + user
            api = NaElement('cifs-share-access-control-create')
            api.child_add_string('permission', 'full_control')
            api.child_add_string('share', share + '_temp$')
            api.child_add_string('user-group-type', 'windows')
            api.child_add_string('user-or-group', smb_user)
            result = netapp.invoke_elem(api)
            ntap_invoke_err_check(result)
            dprint("Share added")
        else:
            dprint("Share already exists")
    elif cmd == "delete":
        api = NaElement('cifs-share-delete')
        api.child_add_string('share-name', share + '_temp$')
        result = netapp.invoke_elem(api)
        ntap_invoke_err_check(result)
        dprint("Share deleted")
    elif cmd == "update":
        api = NaElement('cifs-share-modify')
        api.child_add_string('share', share + '_temp$')
        api.child_add_string('path', domain)    # overloading the domain variable for the path.
        result = netapp.invoke_elem(api)
        ntap_invoke_err_check(result)
        dprint("Updated share path to " + domain)
    return

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
    timeout = 60
    fileset = ""
    sla= ""
    NAS_DA = False
    share = ""
    volume = ""

    running_status_list = ['RUNNING', 'QUEUED', 'ACQUIRING', 'FINISHING', 'TO_CANCEL']

    optlist, args = getopt.getopt(sys.argv[1:], 'hDn:c:t:p:a:f:s:d', ['--help', '--DEBUG', '--creds=', '--ntap_creds=', '--token='
                                  '--pattern=', '--admin=', '--sla=', '--nas_da'])
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
        if opt in ('-a', '--admin'):
            admin_lif = a
        if opt in ('-s', '--sla'):
            sla = a
        if opt in ('d', '--nas_da'):
            NAS_DA = True
    if len(args) == 5:
        try:
            (ntap_host, rubrik_host, volume, share, outfile) = args
        except:
            usage()
    else:
        try:
            (ntap_host, rubrik_host, share, outfile) = args
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
    netapp = NaServer(admin_lif, 1, 15)
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
    if not volume:
        volume = discover_volume(netapp, share)
    api = NaElement('snapshot-list-info')
    api.child_add_string("volume", volume)
    result = netapp.invoke_elem(api)
    ntap_invoke_err_check(result)
    dprint(result.sprintf())
    snaps = result.child_get('snapshots').children_get()
    for s in snaps:
        name = s.child_get_string('name')
        s_time = s.child_get_string('access-time')
        s_time_dt = datetime.fromtimestamp(int(s_time), filer_tz)
        snap_list.append({'name': name, 'time': str(s_time_dt)[:-6]})
    if pattern:
        snap_list = purge_snap_list(snap_list, pattern)
        if not snap_list:
            sys.stderr.write("Pattern yielded no results\n")
            exit(1)
    dprint(snap_list)
    print("NTAP Snapshots:\n")
    for i, s in enumerate(snap_list):
        print(str(i) + ': ' + s['name'] + '\t\t' + str(s['time']))
    index_list_s = python_input("Select snapshots to backup: ")
    index_list = get_index_list(index_list_s, snap_list)
    dprint("INDEX_LIST = " + str(index_list))
    hs_data = rubrik.get('internal', '/host/share', timeout=timeout)
    hs_id = ""
    for hs in hs_data['data']:
        if hs['hostname'] == ntap_host and (hs['exportPoint'] == share or hs['exportPoint'] == share + '_temp$'):
            hs_id = str(hs['id'])
            hs_path_save = share
            rbk_host_id = str(hs['hostId'])
            break
    if not hs_id:
        sys.stderr.write("Can't find share: " + ntap_host + ':' + share + '\n')
        exit(1)
    dprint("HS_ID: " + hs_id)
    if share.startswith('/'):
        protocol = "NFS"
    else:
        protocol = "SMB"
        host_creds = rubrik.get('internal', '/host/share_credential?host_id=' + rbk_host_id, timeout=timeout)
        smb_user = host_creds['data'][0]['username']
        try:
            smb_domain = host_creds['data'][0]['domain']
        except:
            smb_domain = "BUILTIN"
        temp_share('create', netapp, share, smb_domain, smb_user)
        payload = {'exportPoint': share + '_temp$'}
        sh_update = rubrik.patch('internal', '/host/share/' + hs_id, payload, timeout=timeout)
    fs_data = rubrik.get('v1', '/fileset?share_id=' + hs_id, timeout=timeout)
    if fs_data['total'] == 0:
        sys.stderr.write("No Fileset Found on share.\n")
        exit(5)
    elif fs_data['total'] == 1:
        print('\nFound fileset: ' + fs_data['data'][0]['name'] + '  [' + fs_data['data'][0]['configuredSlaDomainName'] + ']')
        use = python_input("Use this fileset [y/n]: ")
        if use[0].lower() != "y":
            exit(1)
        fs_id = str(fs_data['data'][0]['id'])
        fs_index = 0
    else:
        valid = False
        while not valid:
            print('Found multiple filesets on the share.  Choose from the list of filesets:\n')
            for i, f in enumerate(fs_data['data']):
                print(str(i) + ': ' + f['name'] + '  [' + f['configuredSlaDomainName'] + ']')
            fs_index = python_input("Selection: ")
            if int(fs_index) in range(0, len(fs_data['data'])):
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
            sla_data = rubrik.get('v2', '/sla_domain?primary_cluster_id=local', timeout=timeout)
            valid = False
            while not valid:
                print("No SLA available for backups.  Select from the following:")
                for i, s in enumerate(sla_data['data']):
                    print(str(i) + ': ' + s['name'])
                sla_index = python_input("Selection: ")
                if int(sla_index) in range(0, len(sla_data['data'])):
                    sla_id = str(sla_data['data'][int(sla_index)]['id'])
                    valid = True
    dprint("SLA_ID: " + sla_id)
    fp = open(outfile, "a")
    fp.close()
    for i in index_list:
        if protocol == "NFS":
            new_path = update_share_path(rubrik, hs_id, snap_list[int(i)]['name'])
            payload = {'exportPoint': new_path}
            rubrik.patch('internal', '/host/share/' + hs_id, payload, timeout=timeout)
        else:
            update_smb_path(netapp, share + '_temp$', snap_list[int(i)]['name'])
        print("Backing up NTAP snapshot: " + snap_list[int(i)]['name'])
        bu_config = {'slaId': sla_id, 'isPassthrough': NAS_DA}
        dprint("BU_CONFIG: " + str(bu_config))
        bu_status = rubrik.post('v1', '/fileset/' + str(fs_id) + '/snapshot', bu_config, timeout=timeout)
        dprint("JOB: " + str(bu_status))
        bu_status_url = str(bu_status['links'][0]['href']).split('/')
        bu_status_path = "/" + "/".join(bu_status_url[5:])
        bu_time = bu_status['startTime'][:-8]
        bu_done = False
        while not bu_done:
            bu_status = rubrik.get('v1', bu_status_path, timeout=timeout)
            job_status = str(bu_status['status'])
            print("\t STATUS: " + job_status)
            if job_status not in running_status_list:
                if job_status == "SUCCEEDED":
                    bu_done = True
                    break
                else:
                    sys.stderr.write('Job did not complete successfully\n')
                    exit(4)
            time.sleep(15)
        fp = open(outfile, "a")
        fp.write(snap_list[int(i)]['name'] + "," + snap_list[int(i)]['time'] + "," + bu_time + "\n")
        fp.close()
    if protocol == "NFS":
        payload = {'exportPoint': hs_path_save}
        rubrik.patch('internal', '/host/share/' + hs_id, payload, timeout=timeout)
    else:
        payload = {'exportPoint': share}
        sh_update = rubrik.patch('internal', '/host/share/' + hs_id, payload, timeout=timeout)
        temp_share('delete', netapp, share, '', '')





