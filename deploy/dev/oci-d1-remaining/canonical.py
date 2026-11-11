"""Retrieve only this exact fixture run's immutable canonical old-container synthetic data."""
import os
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'scripts/release'))
from v09_prepublication import api,require,retrieve_artifact,write,digest,read
run='43013654037';sha='0ea689c61be521c9ba12a971579b424c64e37e9f';name='oci-canonical-old-'+run
job=api('actions/jobs/130499223328')
require(job['run_id']==int(run) and job['status']=='completed' and job['conclusion']=='success',
        'Exact canonical old-container job did not succeed; wrapper overall failure is not relabelled')
inventory=api(f'actions/runs/{run}/artifacts?per_page=100')
items=[a for a in inventory['artifacts'] if a['name']==name]
require(len(items)==1 and items[0]['id']==13851562928 and items[0]['digest']=='sha256:37d8765e8d49a4e712c3528b11f2ba7828971b8a1bc9dfe1d81f1ca5dea19c35','Canonical old-container artifact missing, ambiguous or changed')
target=Path(sys.argv[1])/'canonical'
retrieve_artifact(items[0],run,sha,target)
meta=read(target/'canonical.json')
require(meta['runId']==run and meta['driverSha']==sha and digest(target/'preupgrade.db')==meta['databaseSha256'],
        'Canonical actual old-container data identity differs')
write(Path(sys.argv[2])/'canonical-input.json',{'runId':run,'driverSha':sha,'artifactId':items[0]['id'],
     'artifactZipSha256':items[0]['digest'],'databaseSha256':meta['databaseSha256'],'baselineImage':meta['baselineImage']})
