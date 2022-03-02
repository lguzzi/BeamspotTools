#!/usr/bin/env python3
from __future__ import print_function
import sys
assert sys.version_info.major>2, "This script requires python 3+"
import os
assert os.system("auth-get-sso-cookie --help &> /dev/null")==0, "auth-get-sso-cookie must be installed. You should run this script on lxplus"
import time
import json
import requests
import http.cookiejar as cookielib
import multiprocessing as mp
import signal

import argparse
parser = argparse.ArgumentParser('''
This script fetches information about stable proton fills form OMS and saves them in a txt file.
Fills are fetched starting from most recent one.''')
parser.add_argument('-n', '--number-of-fills', default=100                      , help="Number of fills to fetch"                     , type=int)
parser.add_argument('-u', '--update'         , default=None                     , help="File to update"                               , type=str)
parser.add_argument('-o', '--output'         , default='output.txt'             , help="Output file"                                  , type=str)
parser.add_argument('-s', '--streams'        , default=max(mp.cpu_count()-4, 1) , help="Number of streams to use for fetching data"   , type=int)
parser.add_argument('-S', '--size'           , default=100                      , help="Number of fills to fetch with a single stream", type=int)
args = parser.parse_args()

def get_cookie(url):
  cookiepath = './cookiefile_OMSfetch.txt'
  print("[INFO] generating cookie for url", URL)
  print("[INFO] ignore the error if a valid cookie already exists")
  cmd = 'auth-get-sso-cookie --url "{}" -o {}'.format(url, cookiepath)
  ret = os.system(cmd)
  cookie = cookielib.MozillaCookieJar(cookiepath)
  cookie.load()
  return cookie

def get_json(url):
  global COOKIE, chunks
  req = requests.get(url, verify=True, cookies=COOKIE, allow_redirects=False)
  jsn = req.json()
  progress.value += 100. / len(chunks)
  time.sleep(.5)
  if not 'data' in jsn.keys():
    return []
  return [j['attributes'] for j in jsn['data'] if j['attributes']['stable_beams'] and 'PROTONS' in j['attributes']['fill_type_runtime']]

progress = mp.Value('f', 0., lock = True)
def pbar():
  global progress
  bar = lambda: '[{}{}%{}]'.format('#'*int(progress.value / 2), int(progress.value), ' '*(50 - int(progress.value / 2)))
  while(True):
    sys.stdout.write('\r%s' %bar())
    sys.stdout.flush()
    time.sleep(.1)
    if progress.value == 100:
      break
  sys.stdout.write('\r%s\n' %bar())
  sys.stdout.flush()

STEP=100
URL="https://cmsoms.cern.ch/agg/api/v1/fills/?sort=-start_time&page[offset]={OFF}&page[limit]={LIM}"
COOKIE=get_cookie('https://cmsoms.cern.ch/')

print('[INFO] Fetching data from OMS')
chunks = sorted(set([_ for _ in range(0, args.number_of_fills, 100)]+[args.number_of_fills]))
with mp.Pool(args.streams) as pool:
  progressbar = mp.Process(target=pbar)
  progressbar.start()
  fetched = pool.map(get_json, [URL.format(OFF=off, LIM=args.size) for off in chunks])
  progressbar.terminate()

if args.update is not None:
  with open(args.update, 'r') as ifile:
    oldmessage = [l.strip('\n') for l in ifile.readlines()[1:]]
    oldfills   = [int(l.split('\t')[0]) for l in oldmessage if len(l)]
else:
  oldfills    = []
  oldmessage  = []

fills = [f for fetch in fetched for f in fetch if f['fill_number'] not in oldfills]
print('[INFO]', len(fills), 'fills fetched.')

format_time     = lambda time      : str(time).replace('T', ' ').replace('Z', '').replace('-', '.')
format_seconds  = lambda seconds   : time.strftime('%H hr %M min', time.gmtime(seconds)) if seconds is not None else str(None)
format_runs     = lambda start, end: ' '.join([str(r) for r in range(start, end+1)]) if None not in [start,end] else str(None)
header_map = {
  'Fill'              : lambda dic: dic['fill_number']                    ,
  'Create Time'       : lambda dic: format_time(dic['start_time'])        , # NOTE: this is the start time, not the stable beam start time
  'Duration Stable'   : lambda dic: format_seconds(dic['duration'])       , # NOTE: 'duration' indicates the stable beam duration
  'Bfield'            : lambda dic: dic['b_field']                        ,
  'Peak Inst Lumi'    : lambda dic: dic['peak_lumi']                      ,
  'Peak Pileup'       : lambda dic: dic['peak_pileup']                    ,
  'Peak Spec Lumi'    : lambda dic: dic['peak_specific_lumi']             ,
  'Delivered Lumi'    : lambda dic: dic['delivered_lumi']                 ,
  'Recorded Lumi'     : lambda dic: dic['recorded_lumi']                  ,
  'Eff By Lumi'       : lambda dic: dic['efficiency_lumi']                ,
  'Eff By Time'       : lambda dic: dic['efficiency_time']                ,
  'Begin Time'        : lambda dic: format_time(dic['start_stable_beam']) , # NOTE: this is the stable beam start time, not the start time
  'to Ready'          : lambda dic: dic['to_ready_time']                  , # OMS: "to HV on". What is this? ms?
  'End Time'          : lambda dic: format_time(dic['end_stable_beam'])   , # NOTE: this is the stable beam end time, not the end time
  'Type'              : lambda dic: dic['fill_type_runtime']              ,
  'Energy'            : lambda dic: dic['energy']                         ,
  'I Beam1 (x10^11)'  : lambda dic: dic['intensity_beam1']                ,
  'I Beam2 (x10^11)'  : lambda dic: dic['intensity_beam2']                ,
  'nB1'               : lambda dic: dic['bunches_beam1']                  ,
  'nB2'               : lambda dic: dic['bunches_beam2']                  ,
  'nCol'              : lambda dic: dic['bunches_colliding']              ,
  'nTar'              : lambda dic: dic['bunches_target']                 ,
  'xIng (micro rad)'  : lambda dic: dic['crossing_angle']                 ,
  'Injection Scheme'  : lambda dic: dic['injection_scheme']               ,
  'Runs'              : lambda dic: format_runs(dic['first_run_number']   , dic['last_run_number']),
  'Comments'          : lambda dic: ''                                    ,
}

header      = ['\t'.join(header_map.keys())]
newmessage  = ['\t'.join([str(val(fill)) for val in header_map.values()]) for fill in fills]
message     = '\n'.join(header+newmessage+oldmessage)

with open(args.output, 'w') as ofile:
  ofile.write(message)