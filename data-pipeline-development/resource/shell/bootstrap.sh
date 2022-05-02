#!/bin/bash
helpFunction()
{
   echo ""
   echo "Usage: $0 -requirements_file=<parameter> "
   echo -e "\t-requirements_file= full path of bootstrap file on S3"
   exit 1 # Exit script after printing help
}

executeBootstrap(){
echo "bootstrap execution start";
sudo aws configure set region us-east-1

sudo aws s3 cp ${1} /home/hadoop/ 
echo "installing python dependencies start";
sudo pip3 install --upgrade pip
sudo /usr/local/bin/pip3 install -r /home/hadoop/requirements.txt
echo "installing python dependencies end"
sudo sed -i -e '$a\export LC_ALL=en_US.UTF-8' ~/.bash_profile
sudo sed -i -e '$a\export LANG=en_US.UTF-8' ~/.bash_profile
source ~/.bash_profile
echo "bootstrap execution end";

}



while [ $# -gt 0 ]; do
  echo $1
  case "$1" in
    -requirements_file=*)
      requirements_file="${1#*=}"
 ;;
    *)
      printf "***************************\n"
      printf "* Error: Invalid argument.*\n"
      printf "***************************\n"
      exit 1
  esac
  shift
done


echo "python library requirements_file: ${requirements_file}"

# Print helpFunction in case parameters are empty
if [ -z "$requirements_file" ] 
then
   echo "Some or all of the parameters are empty";
   helpFunction
else
  executeBootstrap "${requirements_file}" 
fi
echo "Bootstrap Script completed successfully"
 exit 0


