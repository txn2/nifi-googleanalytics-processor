# WIP: NiFi Processor - Google Analytics

Notes:

- Create a [Google Service Account]
- Download JSON credentials
- Enable [analyticsreporting.googleapis.com]
- Create View for a [Google Analytics] property under the admin section.
- Add service account email address as a user under "View Access Management"
- Explore metrics and dimensions with [UA Query Explorer]


[UA Query Explorer]: https://ga-dev-tools.web.app/query-explorer/
[Google Analytics]: https://analytics.google.com/
[Google Service Account]: https://console.cloud.google.com/projectselector2/iam-admin/serviceaccounts?supportedpurview=project
[analyticsreporting.googleapis.com]: https://console.cloud.google.com/apis/api/analyticsreporting.googleapis.com/overview


## Test on Local NiFi Install

1) [Download](https://nifi.apache.org/download.html) and Install a copy of Apache NiFi at `~/workspace/nifi/nifi-1.13.2`
2) Start NiFi: `~/workspace/nifi/nifi-1.15.2/bin/nifi.sh start`
    - Restart NiFi: `~/workspace/nifi/nifi-1.15.2/bin/nifi.sh restart`
    - Stop NiFi: `~/workspace/nifi/nifi-1.15.2/bin/nifi.sh stop`
3) Set password `./bin/nifi.sh set-single-user-credentials user test`
4) Open NiFi: [http://localhost:8080/nifi/](http://localhost:8080/nifi/)

Build from source:
```shell
mvn clean install
cp ./nifi-googleanalytics-nar/target/nifi-googleanalytics-nar-1.13.2.nar \
  ~/workspace/nifi/nifi-1.15.2/lib/
~/workspace/nifi/nifi-1.15.2/bin/nifi.sh restart
```
