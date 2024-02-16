from CRABClient.UserUtilities import config as Configuration
from CRABAPI.RawCommand import crabCommand
from multiprocessing import Pool

import os, sys
import json
import copy
import argparse
parser = argparse.ArgumentParser('''Submit jobs for the VdM scans.
A single job consist of a point (identified by a time window) for a given scan type, coordinate and bunchcrossing.
The lumi range for each time window is used to reduce the list of input files.
The needed information is read from a json file given by the LUMI POG.
Jobs must aggregate events falling into the same time window \
from different datasets into a single fit. 
For this reason, crab is run single-job on a \
list of files (it is used mainly to exploit automatic tape recall).''')
parser.add_argument('--input'         , required=True             , help='timestamp and bunch crossing file from lumi')
parser.add_argument('--storage'       , default='T3_IT_MIB'       , help='storage site')
parser.add_argument('--log'           , default='/dev/null'       , help='log file')
parser.add_argument('--globaltag'     , required=True             , help='global tag')
parser.add_argument('--datasets'      , required=True             , help='datasets to query')
parser.add_argument('--bunchcrossings', required=True             , help='list of bunchcrossings to fit', nargs='+')
parser.add_argument('--streams'       , default=1, type=int       , help='number of streams for DAS queries')
parser.add_argument('--workarea'      , default='BSFit_timerange' , help='crab work area')
args = parser.parse_args()

class Scan:
  '''main class for building a scan object from a LUMI POG json file
  '''
  def __init__(self, label, json, number, datasets, bunchcrossings):
    self.label  = os.path.basename(label).strip('.json')
    self.main   = json
    self.index  = number
    self.scan   = self.main['Scans'][self.index]
    self.dsets  = datasets
    self.bunch  = bunchcrossings
    self.fill   = self.main['Fill'     ]
    self.runn   = self.main['Run'      ][self.index]
    self.stype  = self.main['ScanTypes'][self.index]
    self.sname  = self.main['ScanNames'][self.index]
    self.name   = '_'.join([
      self.label, 
      str(self.fill), 
      str(self.runn), 
      self.stype, 
      self.sname])
    self.points = [Point(scan=self, index=i) for i in range(self.scan['NumPoints'])]

class Point:
  ''' class for building a scan point object
  '''
  def __init__(self, scan, index):
    self.scan     = scan
    self.index    = index
    self.lumib    = self.scan.scan['LSStartTimes'][self.index]
    self.lumie    = self.scan.scan['LSStopTimes' ][self.index]
    self.timeb    = self.scan.scan['StartTimes'  ][self.index]
    self.timee    = self.scan.scan['StopTimes'   ][self.index]
    self.queries  = [self.query(dataset=d, run=self.scan.runn, lumi=l)
      for d in self.scan.dsets 
      for l in range(self.lumib, self.lumie+1) 
    ]
    self.name   = '_'.join([
      self.scan.name,
      str(self.lumib),
      str(self.lumie),
      str(self.index)])
    self.files  = []
    self.jobs   = [Job(point=self, bunchcrossing=b) for b in self.scan.bunch]
  
  def query(self, dataset, run, lumi):
    return 'dasgoclient --query=\"file dataset={DA} run={RN} lumi={LS}\"'.format(DA=dataset, RN=str(run), LS=str(lumi))
  
  def _fetchfiles(self, query):
    '''fetch files from das
    '''
    return [l.strip('\n') for l in os.popen(query).readlines()]
  def poolfetch(self, streams):
    ''' fetch with parallel streams
    '''
    if len(self.files):
      return  #already done for this point
    pool = Pool(streams)
    sys.stdout.write("fetching files for {}...".format(self.name))
    sys.stdout.flush()
    fetched = pool.map(self._fetchfiles, self.queries)
    self.files = [f for r in fetched for f in r]
    sys.stdout.write("\rfetching files for {}...done\n".format(self.name))
    sys.stdout.flush()

class Job:
  ''' class for building a job object (ie. a scan point with a specific bunch crossing)
  '''
  def __init__(self, point, bunchcrossing):
    self.point = point
    self.bunch = bunchcrossing
    self.name = '_'.join([
      self.point.name,
      'bx'+str(self.bunch)])

json      = json.load(open(args.input, 'r'))
datasets  = [d.strip('\n') for d in os.popen('dasgoclient --query="{}"'.format(args.datasets))]
scans     = [Scan(
  label=args.input, 
  json=json, 
  number=i, 
  datasets=datasets, 
  bunchcrossings=args.bunchcrossings) for i,s in enumerate(json['Scans'])
]

jobs = [j for s in scans for p in s.points for j in p.jobs]

config = Configuration()
config.General.workArea         = args.workarea
config.General.transferOutputs  = True
config.JobType.pluginName       = 'Analysis'
config.Data.publication         = False
config.Data.useParent           = False
config.Data.inputDBS            = 'global'
config.Data.splitting           = 'FileBased'
config.Site.storageSite         = args.storage
config.JobType.psetName         = 'BeamFit_custom_workflow.py'
config.JobType.scriptArgs       = ["globalTag={}".format(args.globaltag)]

print("submitting {} crab jobs".format(len(jobs)))

for jj in jobs:
  jj.point.poolfetch(args.streams)
  config.General.requestName  = jj.name
  config.JobType.inputFiles   = jj.point.files
  config.Data.unitsPerJob     = len(jj.point.files)
  config.JobType.outputFiles  = ['{}.txt'.format(jj.name)]
  config.JobType.scriptArgs.append("jobName={}".format(jj.name))
  config.JobType.scriptArgs.append("selectBx={}".format(jj.bunch))
  config.JobType.scriptArgs.append("timerange={S},{E}".format(S=jj.point.timeb,E=jj.point.timee))
  crabCommand('submit', config=config)
import pdb; pdb.set_trace()