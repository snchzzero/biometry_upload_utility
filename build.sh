#!/bin/bash

tar -xvf biometry_upload_utility.tar.xz
tar -xvf biorecord_photos.tar.xz

#python3 -m venv venv

#source venv/bin/activate
source /opt/algont/id/venv/id-services-sync-venv/bin/activate

#tar -xvf requirements.tar.xz -C venv/lib/python3.7/site-packages/
python main.py
