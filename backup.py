#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import datetime
import fabric.api
import shutil
from YaDiskClient.YaDiskClient import YaDisk

backupdir = "/home/vagrant/data/"

sshkey = "/home/vagrant/.ssh/id_rsa"

encpass = "archivepass"

diskparams = {
    "user": "yadiskuser",
    "password": "yadiskpass"
}

servers2backup = {
    "IP": {
        "files": [
            "/data/common/",
            "/data/projects/",
            "/data/services/"
        ],
        "dbs": [
            {
                "name": "dbname",
                "user": "dbuser",
                "password": "dbpass"
            }
        ]
    }
}

def backupfiles(backupdir, servers2backup, sshkey):
    for ip in servers2backup.keys():
        with fabric.api.settings(
                fabric.api.hide('warnings', 'running', 'stdout', 'stderr'),
        ):
            if "files" not in servers2backup[ip].keys():
                print("No files to backup on server {}".format(ip))
                continue
            for f in servers2backup[ip]["files"]:
                try:
                    result = fabric.api.local('/usr/bin/rsync -avz --no-specials --no-devices --ignore-missing-args -e "ssh -i {}" root@{}:{} {}/{}'.format(sshkey, ip, f, backupdir,f.lstrip("/")))
                except:
                    print("Error while copy files")
                    print(result)
    return

def backupdbs(backupdir, servers2backup):
    for ip in servers2backup.keys():
        with fabric.api.settings(
                fabric.api.hide('warnings', 'running', 'stdout', 'stderr'),
        ):
            if "dbs" not in servers2backup[ip].keys():
                print("No dbs to backup on server {}".format(ip))
                continue
            for d in servers2backup[ip]["dbs"]:
                result = fabric.api.local('ssh root@{} "PGPASSWORD=\"{}\" /usr/bin/pg_dump -h 127.0.0.1 -U {} {}" > {}/db/{}.sql'.format(ip, d["password"], d["user"], d["name"], backupdir, d["name"]))
    return

def checkdirs(backupdir):
    backupdirs = ["data", "db"]
    for d in backupdirs:
        try:
            os.makedirs(backupdir + "/" + d + "/")
        except:
            print("{} already exists".format(backupdir + "/" + d + "/"))
    return

def archivebackup(backupdir, encpass):
    with fabric.api.settings(
                fabric.api.hide('warnings', 'running', 'stdout', 'stderr')
        ):
        fabric.api.local('/bin/tar -cz {} | /usr/bin/openssl enc -e -aes-256-cbc -k {} > {}.db.tar.gz.enc'.format(backupdir, encpass, backupdir.rstrip("/")))
    shutil.rmtree(backupdir)
    return

def uploadbackup(backupdir, diskparams, btype):
    disk = YaDisk(diskparams["user"], diskparams["password"])
    yadirs = [x["path"].strip("/") for x in disk.ls("/") if x["isDir"]]
    if "backup" not in yadirs:
        disk.mkdir("/backup/")
    yadirs = [x["path"].strip("/") for x in disk.ls("/backup/") if x["isDir"]]
    if "backup/{}".format(btype) not in yadirs:
        disk.mkdir("/backup/{}".format(btype))
    disk.upload("{}.db.tar.gz.enc".format(backupdir.rstrip("/")), "/backup/{}/{}.db.tar.gz.enc".format(btype, backupdir.strip("/").split("/")[-1]))
    return

def main():
    btypes = {
        "hourly": "%Y-%m-%dT%H:%M:%S",
        "daily": "%Y-%m-%d"
    }
    btype = "hourly"
    if len(sys.argv)>1 and sys.argv[1] in btypes.keys():
        btype = sys.argv[1]
    bsuffix = datetime.datetime.now().strftime(btypes[btype])
    checkdirs(backupdir + "/" + bsuffix)
    backupfiles(backupdir + "/" + bsuffix, servers2backup, sshkey)
    backupdbs(backupdir + "/" + bsuffix, servers2backup)
    archivebackup(backupdir + "/" + bsuffix, encpass)
    uploadbackup(backupdir + "/" + bsuffix, diskparams, btype)
    return


if __name__ == "__main__":
    main()
