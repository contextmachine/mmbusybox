#  Copyright (c) CONTEXTMACHINE 2022.
#  AEC, computational geometry, digital engineering and Optimizing construction processes.
#
#  Author: Andrew Astakhov <sthv@contextmachine.space>, <aa@contextmachine.ru>
#
#  This program is free software; you can redistribute it and/or modify it
#  under the terms of the GNU General Public License as published by the
#  Free Software Foundation; either version 2 of the License, or (at your
#  option) any later version.  See http://www.gnu.org/copyleft/gpl.html for
#  the full text of the license.
#
#
import json
from cxm.buckets.sessions import S3Session


class AppSession(S3Session):

    def __init__(self, buck=None):
        super().__init__(bucket=buck)

        self.view = "tmp/"
        self.full_icon = ""
        self.icon = ""
        self.scenes = "scenes/"
        self.__dict__ |= self.config

    @property
    def config(self):
        return json.loads(self.s3.get_object(Bucket=self.bucket, Key="config.json")["Body"].read())

    def get_icon(self):
        rspns = self.s3.get_object(Bucket=self.bucket, Key=self.icon)["Body"]
        yield from rspns

    def logo(self):
        rspns = self.s3.get_object(Bucket=self.bucket, Key=self.full_icon)["Body"]
        yield from rspns

    def get_object(self, name, pref):
        return self.s3.get_object(Bucket=self.bucket, Key=pref + name)["Body"]

    def get_list_objects(self, pref):
        return self.s3.list_objects(Bucket=self.bucket, Prefix=pref)["Contents"]

    def get_keys(self, pref):
        ans = []
        [ans.append(m) if not (m == "") else None for m in
         map(lambda x: x["Key"].split("/")[-1], self.get_list_objects(pref))]
        return ans

    def put_object(self, name, data, pref):
        self.s3.put_object(Key=pref + name, Bucket=self.bucket, Body=data)

    def stream_object(self, name, pref):
        obj = self.s3.get_object(Bucket=self.bucket, Key=pref + name)["Body"]
        yield from obj

    def delete_object(self, name, pref):
        response = self.s3.delete_object(Bucket=self.bucket, Key=pref + name)
        del response['ResponseMetadata']
        return response
