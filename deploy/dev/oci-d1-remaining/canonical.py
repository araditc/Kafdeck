"""Retrieve only this exact fixture run's immutable canonical old-container synthetic data."""
import os
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'scripts/release'))
from v09_prepublication import api,require,retrieve_artifact,write,digest,read
run=os.environ['GITHUB_RUN_ID'];sha=os.environ['GITHUB_SHA'];name='oci-canonical-old-'+run
inventory=api(f'actions/runs/{run}/artifacts?per_page=100')
items=[a for a in inventory['artifacts'] if a['name']==name]
require(len(items)==1,'Canonical old-container artifact missing or ambiguous')
target=Path(sys.argv[1])/'canonical'
retrieve_artifact(items[0],run,sha,target)
meta=read(target/'canonical.json')
require(meta['runId']==run and meta['driverSha']==sha and digest(target/'preupgrade.db')==meta['databaseSha256'],
        'Canonical actual old-container data identity differs')
write(Path(sys.argv[2])/'canonical-input.json',{'runId':run,'driverSha':sha,'artifactId':items[0]['id'],
     'artifactZipSha256':items[0]['digest'],'databaseSha256':meta['databaseSha256'],'baselineImage':meta['baselineImage']})
