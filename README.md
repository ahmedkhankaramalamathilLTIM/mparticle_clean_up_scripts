# mparticle_clean_up_scripts
Cleaning up Scripts which will be executed on mParticle for Data Services works
# mParticle Email Identity & Event Updater

This project provides two Python scripts to sequentially:

1. **Update email identities** for anonymous profiles using mParticle's `/modify` API.
2. **Send bulk events** to mParticle for the same profiles using the updated identities.

---

## 📁 Files

- `updateAnonymousProfilesEmailIdentity.py`  
  Updates email identity for each `mpid` using the `/modify` endpoint.
  
- `bulk_events_with_identity.py`  
  Sends event payloads for each `mpid`, using the same input.

---

## 📝 Prerequisites

- Python 3.x
- Required libraries (install via pip if not already installed):
  ```bash
  pip install requests pandas

🚀 How to Run
Make sure to run the scripts in the following order.
python updateAnonymousProfilesEmailIdentity.py

This script:

Reads anonymousprofiles.csv

Calls mParticle’s /modify API to update the email identity for each mpid

2. Send Bulk Events


python bulk_events_with_identity.py

This script:

Reads anonymousprofiles.csv

Sends event payloads for each mpid using the updated email identity


