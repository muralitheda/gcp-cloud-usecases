
## Download and Install GCloud SDK ##

1. **Go to the below URL**  
https://cloud.google.com/sdk/docs/install-sdk#linux 

2. **In Windows/Mac**
Copy the link provided above and import the tar inside the VM/Windows
Download and install the software in your Windows/Mac PC
Open command/terminal prompt
```bash
gcloud auth login
```

3. **Inside the VM**
Import -> untar -> install -> N -> Y -> enter -> export
```bash
cd /home/hduser/install
curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz
tar -xvf google-cloud-cli-linux-x86_64.tar.gz
/home/hduser/install/google-cloud-sdk/install.sh
```

Do you want to help improve the Google Cloud CLI (y/N)? **N** 
Modify profile to update your $PATH and enable shell command completion? Do you want to continue (Y/n)?  **Y** 
Enter a path to an rc file to update, or leave blank to use [/home/hduser/.bashrc]: **just type enter key**
Run the below command finally in your command prompt:
```bash
source ~/.bashrc
```