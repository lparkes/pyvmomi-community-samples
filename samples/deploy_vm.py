#!/usr/bin/env python

# Make sure that the datastore cluster has EnforceStorageProfiles=2
# set in the advanced options.

# If you are running this with Python 2, you will need the "future"
# package installed. e.g. with pip install future.

from __future__ import print_function

import atexit
import argparse
import getpass
import pprint
import ssl
import sys

from http.cookies import SimpleCookie

from pyVmomi import pbm, VmomiSupport, SoapStubAdapter, vim
from pyVim.connect import SmartConnect, Disconnect
from pyVim.task import WaitForTask

"""
Example of creating a Guest with file placement controlled by Storage Polices
"""

__author__ = 'Lloyd Parkes'

def main():
    args = GetArgs()
    if args.password:
        password = args.password
    else:
        password = getpass.getpass(
            prompt='Enter password for host %s and '
                   'user %s: ' % (args.host, args.user))


    # Get vSphere Service Instance object
    si = GetServiceInstance(args, password)
    content = si.RetrieveContent()
    
    # Connect to SPBM Endpoint
    pbmSi = GetPbmServiceInstance(si)
    pbmContent = pbmSi.RetrieveContent()

    # Get Storage Policy IDs
    bronze_storage = StoragePolicy(pbmContent.profileManager, 'Bronze Storage')
    bronze_profile = [vim.vm.DefinedProfileSpec(profileId=bronze_storage)]

    silver_storage = StoragePolicy(pbmContent.profileManager, 'Silver Storage')
    silver_profile = [vim.vm.DefinedProfileSpec(profileId=silver_storage)]

    gold_storage = StoragePolicy(pbmContent.profileManager, 'Gold Storage')
    gold_profile = [vim.vm.DefinedProfileSpec(profileId=gold_storage)]

    disk_policies = [None, gold_profile, silver_profile, bronze_profile]
    
    # Build config spec
    datacenter = content.rootFolder.childEntity[0]
    vmfolder = datacenter.vmFolder
    hosts = datacenter.hostFolder.childEntity
    resource_pool = hosts[0].resourcePool    
    vm_name = 'DC-Flash'
    network_name = 'VM Network'
    datastore_name = 'DatastoreCluster'

    config = vim.vm.ConfigSpec(name=vm_name, memoryMB=2048, numCPUs=2,
                               guestId='ubuntu64Guest',
                               version='vmx-11')
    config.vmProfile = bronze_profile

    nic = HelperCreateNIC()
    nic.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
    nic.device.backing = vim.vm.device.VirtualEthernetCard.NetworkBackingInfo()
    nic.device.backing.network = FindObj(content, [vim.Network], network_name)
    nic.device.backing.deviceName = network_name
    config.deviceChange.append(nic)

    scsi_ctl = HelperCreateHBA()
    config.deviceChange.append(scsi_ctl)

    scsi_id = 0
    for policy in disk_policies:
        diskspec = HelperCreateDisk(scsi_ctl, scsi_id)
        diskspec.profile = policy
        config.deviceChange.append(diskspec)
        scsi_id += 1
    
    # PodSelectionSpec.storagePod is the user selected SDRS pod for the VM, i.e., its system files.
    # PodSelectionSpec.disk.storagePod is the user selected SDRS pod for the given disk

    # For CreateVm and AddDisk, the manually selected datastore must be
    # specified in ConfigSpec.files or
    # ConfigSpec.deviceChange.device.backing.datastore, the fields should will
    # be unset if the user wants SDRS to recommend the datastore.

    datastore_cluster = FindObj(content, [vim.StoragePod], datastore_name)

    vmPodConfig = vim.storageDrs.PodSelectionSpec.VmPodConfig()
    vmPodConfig.storagePod = datastore_cluster

    for disk in config.deviceChange:
        if isinstance (disk.device, vim.vm.device.VirtualDisk):
             disk_loc = vim.storageDrs.PodSelectionSpec.DiskLocator()
             disk_loc.diskId = disk.device.key
             disk_loc.diskBackingInfo = disk.device.backing
             vmPodConfig.disk.append(disk_loc)

    pod_sel_spec = vim.storageDrs.PodSelectionSpec()
    pod_sel_spec.storagePod = datastore_cluster
    pod_sel_spec.initialVmConfig = [vmPodConfig]
    
    storage_spec = vim.storageDrs.StoragePlacementSpec()
    storage_spec.podSelectionSpec = pod_sel_spec
    storage_spec.configSpec = config
    storage_spec.resourcePool = resource_pool
    storage_spec.host = hosts[0].host[0]
    storage_spec.folder = vmfolder
    storage_spec.type = 'create'

    # Deploy the VM
   
    rec = content.storageResourceManager.RecommendDatastores(storageSpec=storage_spec)
    if len(rec.recommendations) > 0:
        preferred_rec = rec.recommendations[0]
    
        print ("Creating VM {}...".format(vm_name))
        task = content.storageResourceManager.ApplyStorageDrsRecommendation_Task(preferred_rec.key)
        WaitForTask(task)
    else:
        print (rec.drsFault.reason)
        for fault in rec.drsFault.faultsByVm:
            print (fault.fault[0].msg)

def HelperCreateNIC():
    nic = vim.vm.device.VirtualDeviceSpec()
    nic.device = vim.vm.device.VirtualVmxnet3()
    nic.device.wakeOnLanEnabled = True
    nic.device.deviceInfo = vim.Description()
    nic.device.deviceInfo.label = 'NIC 1'
    nic.device.deviceInfo.summary = 'vmxnet3'
    nic.device.connectable = vim.vm.device.VirtualDevice.ConnectInfo()
    nic.device.connectable.startConnected = True
    nic.device.connectable.allowGuestControl = True
    nic.device.connectable.connected = True
    nic.device.addressType = 'generated'

    return nic

def HelperCreateHBA():
    scsi_ctl = vim.vm.device.VirtualDeviceSpec()
    scsi_ctl.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
    scsi_ctl.device = vim.vm.device.ParaVirtualSCSIController()

    scsi_ctl.device.deviceInfo = vim.Description()
    scsi_ctl.device.slotInfo = vim.vm.device.VirtualDevice.PciBusSlotInfo()
    scsi_ctl.device.slotInfo.pciSlotNumber = 16
    scsi_ctl.device.key = KeyMaker.next()
    scsi_ctl.device.unitNumber = 3
    scsi_ctl.device.busNumber = 0
    scsi_ctl.device.hotAddRemove = True
    scsi_ctl.device.sharedBus = 'noSharing'
    scsi_ctl.device.scsiCtlrUnitNumber = 7
    
    return scsi_ctl

def HelperCreateDisk(scsi_ctl, scsi_id):
    diskspec = vim.vm.device.VirtualDeviceSpec()
    diskspec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
    diskspec.fileOperation = vim.vm.device.VirtualDeviceSpec.FileOperation.create
    diskspec.device = vim.vm.device.VirtualDisk()
    diskspec.device.key = KeyMaker.next()
    diskspec.device.backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
    diskspec.device.backing.diskMode = 'persistent'
    diskspec.device.backing.thinProvisioned = True
    diskspec.device.capacityInKB = 10*1024*1024   # 10GB

    diskspec.device.controllerKey = scsi_ctl.device.key
    diskspec.device.unitNumber = scsi_id

    return diskspec

# Keys are assigned by the server, but we sometimes need temporary
# keys when creating stuff. This class creates keys for this
# purpose. Negative values are used so that they won't conflict with
# server assigned values.
class KeyMaker:
    key = -100
    @staticmethod
    def next():
        KeyMaker.key -= 1
        return KeyMaker.key

def StoragePolicy(profileManager, profileName):
    profileIds = profileManager.PbmQueryProfile(
        resourceType=pbm.profile.ResourceType(resourceType="STORAGE"),
        profileCategory="REQUIREMENT")

    if len(profileIds) > 0:
        profiles = profileManager.PbmRetrieveContent(profileIds=profileIds)
        for profile in profiles:
            if profileName == profile.name:
                return profile.profileId.uniqueId

def FindObj(content, vimtype, name):
    container = content.viewManager.CreateContainerView(container=content.rootFolder, recursive=True, type=vimtype)
    obj_list = container.view
    container.Destroy()

    for obj in obj_list:
        if obj.name == name:
            return obj
                
def GetArgs():
    """
    Supports the command-line arguments listed below.
    """
    parser = argparse.ArgumentParser(
        description='Process args for guest deployment sample application')
    parser.add_argument('-s', '--host', required=True, action='store',
                        help='Remote host to connect to')
    parser.add_argument('-o', '--port', type=int, default=443, action='store',
                        help='Port to connect on')
    parser.add_argument('-u', '--user', required=True, action='store',
                        help='User name to use when connecting to host')
    parser.add_argument('-p', '--password', required=False, action='store',
                        help='Password to use when connecting to host')
    args = parser.parse_args()
    return args

# retrieve SPBM API endpoint from a normal API endpoint
def GetPbmServiceInstance(si):
    vpxdStub = si._stub
    sessionCookie = vpxdStub.cookie.split('"')[1]
    httpContext = VmomiSupport.GetHttpContext()
    cookie = SimpleCookie()
    cookie["vmware_soap_session"] = sessionCookie
    httpContext["cookies"] = cookie
    VmomiSupport.GetRequestContext()["vcSessionCookie"] = sessionCookie
    hostname = vpxdStub.host.split(":")[0]

    context = None
    if hasattr(ssl, "_create_unverified_context"):
        context = ssl._create_unverified_context()
    pbmStub = SoapStubAdapter(
        host=hostname,
        version="pbm.version.version1",
        path="/pbm/sdk",
        poolSize=0,
        sslContext=context)

    return pbm.ServiceInstance("ServiceInstance", pbmStub)

def GetServiceInstance(args, password):
    context = None
    if hasattr(ssl, "_create_unverified_context"):
        context = ssl._create_unverified_context()
    si = SmartConnect(host=args.host,
                      user=args.user,
                      pwd=password,
                      port=int(args.port),
                      sslContext=context)

    atexit.register(Disconnect, si)

    return si

main()
