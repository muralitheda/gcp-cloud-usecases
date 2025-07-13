## GCS Authentication

a. **To login into a given/different user account (DON’T do gcloud init again)**
```bash
gcloud auth revoke
gcloud auth login
```
 
https://accounts.google.com/o/oauth2/auth?response_type=code&client_id=325559ddd40559.apps.googleusercontent.com...  

b. **To set a project (use the projectid to set a given project)**
```bash
gcloud config set project gcs-proj-2025
```

c. **To list all properties of the active configuration**
```bash
gcloud config list
```

d. **To list all authenticated list of users/principals**
```bash
gcloud auth list
```

e. **To set an user account (if not already logged in then we have to use gcloud auth login to login again)**
```bash
gcloud config set account your@mailid.com
```
OR
#Connecting by using the Service Account

f. **To activate the service account in the current vm (authenticating & authorization)**
```bash
gcloud auth revoke
gcloud auth activate-service-account --key-file=/Users/murali/Downloads/iz-cloud-project.json
gcloud config list
```

g. **Setting for Active Configuration for easy use/access for ONETIME**
```bash
Gcloud init 
Pick configuration to use:
 [1] Re-initialize this configuration [default] with new settings 
 [2] Create a new configuration
Please enter your numeric choice:  2

Enter configuration name. Names start with a lower case letter and contain only lower case letters a-z, digits 0-9, and hyphens '-':  murali-config-1

Select an account:
 [1] iz-ctp-serviceaccount@iz-cloud-project.iam.gserviceaccount.com
 [2] Sign in with a new Google Account
 [3] Skip this step
Please enter your numeric choice:  1

API [cloudresourcemanager.googleapis.com] not enabled on project [162036903753]. Would you like to enable and retry (this will take a few minutes)? (y/N)?  y

Pick cloud project to use: 
 [1] iz-cloud-project
 [2] Enter a project ID
 [3] Create a new project
Please enter numeric choice or text value (must exactly match list item):  1

Do you want to configure a default Compute Region and Zone? (Y/n)?  Y

Please enter numeric choice or text value (must exactly match list item):  9
Your project default Compute Engine zone has been set to [us-central1-a].

gcloud config list
[compute]
region = us-central1
zone = us-central1-a
[core]
account = iz-ctp-serviceaccount@iz-cloud-project.iam.gserviceaccount.com
disable_usage_reporting = True
project = iz-cloud-project
Your active configuration is: [murali-config-1]
```

h. **To logout of the given user/principals account**
```bash
gcloud auth revoke
```
